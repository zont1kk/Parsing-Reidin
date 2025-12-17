from playwright.sync_api import sync_playwright
import json
import os

def load_config():
    config_file = os.path.join(os.getcwd(), "config.json")
    if not os.path.exists(config_file):
        print("[ERROR] config.json не найден")
        return {}
    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config

config = load_config()
auth_config = config["auth"]
DEVICE_ID = auth_config["device_id"]
USERNAME = auth_config["username"]
PASSWORD = auth_config["password"]

def load_areas():
    if not os.path.exists("areas.txt"):
        print("[ERROR] Файл areas.txt не найден")
        return []
    with open("areas.txt", "r", encoding="utf-8") as f:
        areas = [line.strip() for line in f if line.strip()]
    print(f"[OK] Загружено {len(areas)} районов")
    return areas

def main():
    areas = load_areas()
    if not areas:
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        page.goto("https://insight.reidin.com/", wait_until="load", timeout=120000)
        page.evaluate(f"localStorage.setItem('deviceId', '{DEVICE_ID}');")
        print("[OK] device_id установлен")

        page.goto("https://insight.reidin.com/auth/login", wait_until="load", timeout=120000)
        print("[OK] Страница логина открыта")

        page.fill('#input-emaillogin-desktop', USERNAME)
        page.fill('#input-passwordlogin-desktop', PASSWORD)
        page.locator('xpath=//input[@id="input-emaillogin-desktop"]/ancestor::form[1]//button[@type="submit"]').click()
        page.wait_for_load_state("networkidle", timeout=120000)
        context.storage_state(path="state.json")
        print("[OK] Авторизация успешна")

        page.goto("https://insight.reidin.com/home/dashboard/754", wait_until="load", timeout=120000)
        print("[OK] Dashboard открыт")

        try:
            page.wait_for_load_state("networkidle", timeout=120000)
        except:
            pass

        page.wait_for_timeout(10000)
        print("[OK] Dashboard загружен")

        output_file = "captured_requests.json"
        all_captured = {}

        print(f"\n[INFO] Начинаю обработку {len(areas)} районов...")

        for area in areas:
            print(f"\n{'='*60}")
            print(f"[PROCESSING] {area}")
            print(f"{'='*60}")

            dropdown_menu = page.locator('div.slicer-dropdown-menu[aria-label*="Area, Community"]')
            if dropdown_menu.count() == 0:
                for frame in page.frames:
                    dropdown_menu = frame.locator('div.slicer-dropdown-menu[aria-label*="Area, Community"]')
                    if dropdown_menu.count() > 0:
                        break

            if dropdown_menu.count() == 0:
                print(f"[ERROR] Не найден dropdown для района: {area}")
                continue

            dropdown_menu.first.click()
            page.wait_for_timeout(2000)

            search_header = page.locator('div.searchHeader.show')
            if search_header.count() == 0:
                for frame in page.frames:
                    search_header = frame.locator('div.searchHeader.show')
                    if search_header.count() > 0:
                        break

            search_input = search_header.locator('input.searchInput')
            if search_input.count() > 0:
                search_input.first.clear()
                page.wait_for_timeout(300)

                for char in area:
                    search_input.first.type(char)
                    page.wait_for_timeout(80)

                page.wait_for_timeout(2000)

                scroll_region = page.locator('div.scrollRegion')
                if scroll_region.count() == 0:
                    for frame in page.frames:
                        scroll_region = frame.locator('div.scrollRegion')
                        if scroll_region.count() > 0:
                            break

                all_rows = scroll_region.locator('div.row')
                target_element = None

                for i in range(all_rows.count()):
                    row = all_rows.nth(i)
                    element_with_title = row.locator(f'[title="{area}"]')
                    if element_with_title.count() > 0:
                        target_element = element_with_title.first
                        break
                    all_elements = row.locator('*')
                    for j in range(all_elements.count()):
                        elem = all_elements.nth(j)
                        try:
                            if area in elem.text_content():
                                target_element = elem
                                break
                        except:
                            pass
                    if target_element:
                        break

                if not target_element and all_rows.count() > 0:
                    first_row = all_rows.first
                    first_elem = first_row.locator('*').first
                    if first_elem.count() > 0:
                        target_element = first_elem

                if target_element:
                    print(f"[INFO] Найден район: {area}")
                    print(f"[INFO] Подключаю обработчик для перехвата запросов...")

                    captured_requests = []

                    def capture_response(response):
                        if "/query" in response.url:
                            try:
                                request_data = response.request.post_data
                                response_data = response.json()

                                if request_data:
                                    request_json = json.loads(request_data)
                                    captured_requests.append({
                                        "request": request_json,
                                        "response": response_data
                                    })
                                    print(f"      [CAPTURED] Запрос #{len(captured_requests)}")
                            except Exception as e:
                                print(f"      [ERROR] Ошибка при перехвате: {e}")

                    page.on("response", capture_response)
                    print(f"[INFO] Обработчик подключен, ловлю запросы перед кликом...")

                    page.wait_for_timeout(500)
                    target_element.click()
                    print(f"[INFO] Клик выполнен, ждем запросов...")

                    try:
                        page.wait_for_load_state("networkidle", timeout=120000)
                    except:
                        page.wait_for_timeout(3000)

                    page.remove_listener("response", capture_response)
                    print(f"[OK] Перехвачено {len(captured_requests)} запросов для {area}")

                    all_captured[area] = captured_requests

                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(all_captured, f, ensure_ascii=False, indent=2)
                    print(f"[OK] Сохранено в {output_file}")

                else:
                    print(f"[WARNING] Не найден элемент для района: {area}")
            else:
                print(f"[WARNING] Не найден инпут поиска для района: {area}")

        browser.close()
        print(f"\n[OK] Захват завершен. Всего районов: {len(all_captured)}")

if __name__ == "__main__":
    main()
