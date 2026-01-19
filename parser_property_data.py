from playwright.sync_api import sync_playwright
import json
import os
from datetime import datetime

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

DASHBOARD_URL = "https://insight.reidin.com/home/dashboard/996"

def cleanup_old_files():
    print("[CLEANUP] Удаление старых файлов перед запуском...")
    patterns = ['*_property_data_*.xlsx', '*_property_data_*.json']
    exclude = ['sales_property_data.json', 'rent_property_data.json']
    removed_count = 0
    for pattern in patterns:
        import glob
        for file in glob.glob(pattern):
            if os.path.basename(file) not in exclude:
                try:
                    os.remove(file)
                    removed_count += 1
                    print(f"[OK] Удален: {os.path.basename(file)}")
                except Exception as e:
                    print(f"[WARNING] Не удалось удалить {file}: {e}")
    print(f"[OK] Удалено старых файлов: {removed_count}\n")

def main():
    cleanup_old_files()
    print(f"[START] Запуск парсера Property Data")
    print(f"[INFO] Dashboard: {DASHBOARD_URL}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True
        )
        context = browser.new_context()
        page = context.new_page()

        page.wait_for_timeout(3000)
        context.storage_state(path="state.json")

        page.goto("https://insight.reidin.com/auth/login", wait_until="load", timeout=60000)
        print("[OK] Страница логина открыта")

        page.evaluate(f"localStorage.setItem('deviceId', '{DEVICE_ID}');")
        print("[OK] device_id установлен")

        page.fill('#input-emaillogin-desktop', USERNAME)
        page.fill('#input-passwordlogin-desktop', PASSWORD)

        page.locator('xpath=//input[@id="input-emaillogin-desktop"]/ancestor::form[1]//button[@type="submit"]').click()

        try:
            page.wait_for_load_state("networkidle", timeout=60000)
        except:
            print("[WARNING] Timeout после логина, продолжаю...")
            page.wait_for_timeout(5000)

        context.storage_state(path="state.json")
        print("[OK] Авторизация успешна. state.json сохранён.")

        try:
            page.goto(DASHBOARD_URL, timeout=60000)
        except:
            print("[WARNING] Timeout при загрузке dashboard, продолжаю...")

        print("[OK] Dashboard открыт")

        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except:
            print("[WARNING] Timeout ожидания networkidle, продолжаю...")

        print("[>>] Жду загрузки элементов Power BI...")
        max_wait = 30
        found = False

        for attempt in range(max_wait):
            page.wait_for_timeout(2000)

            for frame in page.frames:
                dropdowns = frame.locator('div.slicer-dropdown-menu')
                if dropdowns.count() > 0:
                    print(f"[OK] Элементы загружены (попытка {attempt + 1})")
                    found = True
                    break

            if found:
                break

        if not found:
            print("[WARNING] Элементы не загрузились за 30 секунд, но продолжаю...")

        page.wait_for_timeout(2000)
        print("[OK] Dashboard готов к работе")

        print("\n[>>] Ищу Property dropdown...")
        property_dropdown = page.locator('div.slicer-dropdown-menu[aria-label="Property"]')

        if property_dropdown.count() == 0:
            print("[INFO] Ищу в фреймах...")
            for frame in page.frames:
                property_dropdown = frame.locator('div.slicer-dropdown-menu[aria-label="Property"]')
                if property_dropdown.count() > 0:
                    print(f"[OK] Найден в фрейме: {frame.name}")
                    break

        if property_dropdown.count() == 0:
            print("[ERROR] Property dropdown не найден")
            print("[INFO] Браузер остается открытым для проверки. Нажмите Enter для завершения...")
            input()
            context.close()
            return

        print("[>>] Кликаю по Property dropdown...")
        property_dropdown.first.click()
        print("[OK] Клик выполнен")

        page.wait_for_timeout(2000)

        print("\n[>>] Ищу кнопку 'Выбрать все'...")
        select_all = page.locator('div[title="Выбрать все"], div[title="Select all"]')

        if select_all.count() == 0:
            print("[INFO] Ищу в фреймах...")
            for frame in page.frames:
                select_all = frame.locator('div[title="Выбрать все"], div[title="Select all"]')
                if select_all.count() > 0:
                    print(f"[OK] Найден в фрейме: {frame.name}")
                    break

        if select_all.count() == 0:
            print("[ERROR] Кнопка 'Выбрать все' не найдена")
            return

        print("[>>] Кликаю 'Выбрать все'...")
        select_all.first.click()
        print("[OK] Клик выполнен")

        page.wait_for_timeout(2000)

        print("\n[>>] Закрываю Property dropdown...")
        page.keyboard.press("Escape")
        page.wait_for_timeout(2000)
        print("[OK] Dropdown закрыт")

        print("\n[>>] Устанавливаю дату начала: 01.01.2003...")
        date_start_input = page.locator('input[aria-label^="Дата начала"]')
        if date_start_input.count() == 0:
            print("[INFO] Ищу в фреймах...")
            for frame in page.frames:
                date_start_input = frame.locator('input[aria-label^="Дата начала"]')
                if date_start_input.count() > 0:
                    print(f"[OK] Найден в фрейме: {frame.name}")
                    break

        if date_start_input.count() > 0:
            date_start_input.first.clear()
            page.wait_for_timeout(2000)
            date_start_input.first.fill("01.01.2003")
            page.wait_for_timeout(2000)
            print("[OK] Дата начала установлена: 01.01.2003")

            try:
                page.wait_for_load_state("networkidle", timeout=2000)
            except:
                pass
            page.wait_for_timeout(2000)
        else:
            print("[WARNING] Поле 'Дата начала' не найдено")

        print("\n" + "="*70)
        print("[START] ПОЛУЧЕНИЕ СПИСКА ГОРОДОВ")
        print("="*70)

        city_dropdown = page.locator('div.slicer-dropdown-menu[aria-label="City, Community"]')
        if city_dropdown.count() == 0:
            print("[INFO] Ищу City dropdown в фреймах...")
            for frame in page.frames:
                city_dropdown = frame.locator('div.slicer-dropdown-menu[aria-label="City, Community"]')
                if city_dropdown.count() > 0:
                    print(f"[OK] Найден в фрейме: {frame.name}")
                    break

        if city_dropdown.count() == 0:
            print("[ERROR] City dropdown не найден")
            context.close()
            return

        print("[>>] Кликаю по City dropdown...")
        city_dropdown.first.click()
        page.wait_for_timeout(2000)
        print("[OK] City dropdown открыт")

        dropdown_popup = None
        all_popups = page.locator('div.slicer-dropdown-popup.focused')
        if all_popups.count() == 0:
            for frame in page.frames:
                all_popups = frame.locator('div.slicer-dropdown-popup.focused')
                if all_popups.count() > 0:
                    break

        for i in range(all_popups.count()):
            popup = all_popups.nth(i)
            display_style = popup.evaluate('el => window.getComputedStyle(el).display')
            if display_style == 'block':
                dropdown_popup = popup
                break

        cities = []
        if dropdown_popup:
            scroll_region = dropdown_popup.locator('div.scrollRegion')
            if scroll_region.count() > 0:
                all_rows = scroll_region.locator('div.row')
                for i in range(all_rows.count()):
                    row = all_rows.nth(i)
                    slicer_item = row.locator('div.slicerItemContainer')
                    if slicer_item.count() > 0:
                        title = slicer_item.first.get_attribute('title')
                        if title and title != "Выбрать все" and title != "Select all":
                            cities.append(title)

        print(f"[OK] Найдено городов: {len(cities)}")
        print(f"[INFO] Города: {', '.join(cities)}")

        page.keyboard.press("Escape")
        page.wait_for_timeout(2000)

        downloaded_files = []

        for city in cities:
            print("\n" + "="*70)
            print(f"[CITY] {city}")
            print("="*70)

            city_dropdown = page.locator('div.slicer-dropdown-menu[aria-label="City, Community"]')
            if city_dropdown.count() == 0:
                for frame in page.frames:
                    city_dropdown = frame.locator('div.slicer-dropdown-menu[aria-label="City, Community"]')
                    if city_dropdown.count() > 0:
                        break

            city_dropdown.first.click()
            page.wait_for_timeout(2000)

            dropdown_popup = None
            all_popups = page.locator('div.slicer-dropdown-popup.focused')
            if all_popups.count() == 0:
                for frame in page.frames:
                    all_popups = frame.locator('div.slicer-dropdown-popup.focused')
                    if all_popups.count() > 0:
                        break

            for i in range(all_popups.count()):
                popup = all_popups.nth(i)
                display_style = popup.evaluate('el => window.getComputedStyle(el).display')
                if display_style == 'block':
                    dropdown_popup = popup
                    break

            scroll_region = dropdown_popup.locator('div.scrollRegion') if dropdown_popup else page.locator('div.scrollRegion')
            if scroll_region.count() == 0:
                for frame in page.frames:
                    scroll_region = frame.locator('div.scrollRegion')
                    if scroll_region.count() > 0:
                        break

            all_rows = scroll_region.locator('div.row')
            city_clicked = False
            for i in range(all_rows.count()):
                row = all_rows.nth(i)
                slicer_item = row.locator('div.slicerItemContainer')
                if slicer_item.count() > 0:
                    title = slicer_item.first.get_attribute('title')
                    if title == city:
                        print(f"[>>] Кликаю по городу: {city}")
                        slicer_item.first.click()
                        page.wait_for_timeout(2000)
                        city_clicked = True
                        break

            if not city_clicked:
                print(f"[ERROR] Не удалось кликнуть по городу: {city}")
                page.keyboard.press("Escape")
                page.wait_for_timeout(2000)
                continue

            page.keyboard.press("Escape")
            page.wait_for_timeout(2000)

            try:
                page.wait_for_load_state("networkidle", timeout=2000)
            except:
                pass
            page.wait_for_timeout(2000)

            print("\n[>>] Получаю список типов для города: " + city)

            subtype_dropdown = page.locator('div.slicer-dropdown-menu[aria-label="Property Subtype"]')
            if subtype_dropdown.count() == 0:
                for frame in page.frames:
                    subtype_dropdown = frame.locator('div.slicer-dropdown-menu[aria-label="Property Subtype"]')
                    if subtype_dropdown.count() > 0:
                        break

            if subtype_dropdown.count() == 0:
                print(f"[ERROR] Property Subtype dropdown не найден для города {city}")
                continue

            subtype_dropdown.first.click()
            page.wait_for_timeout(2000)

            dropdown_popup = None
            all_popups = page.locator('div.slicer-dropdown-popup.focused')
            if all_popups.count() == 0:
                for frame in page.frames:
                    all_popups = frame.locator('div.slicer-dropdown-popup.focused')
                    if all_popups.count() > 0:
                        break

            for i in range(all_popups.count()):
                popup = all_popups.nth(i)
                display_style = popup.evaluate('el => window.getComputedStyle(el).display')
                if display_style == 'block':
                    dropdown_popup = popup
                    break

            property_types = []
            if dropdown_popup:
                scroll_region = dropdown_popup.locator('div.scrollRegion')
                if scroll_region.count() > 0:
                    all_rows = scroll_region.locator('div.row')
                    for i in range(all_rows.count()):
                        row = all_rows.nth(i)
                        slicer_item = row.locator('div.slicerItemContainer')
                        if slicer_item.count() > 0:
                            title = slicer_item.first.get_attribute('title')
                            if title and title != "Выбрать все" and title != "Select all":
                                property_types.append(title)

            print(f"[OK] Найдено типов для {city}: {len(property_types)}")
            print(f"[INFO] Типы: {', '.join(property_types)}")

            page.keyboard.press("Escape")
            page.wait_for_timeout(2000)

            for prop_type in property_types:
                print(f"\n[PROPERTY TYPE] {prop_type}")
                print("-" * 60)

                subtype_dropdown = page.locator('div.slicer-dropdown-menu[aria-label="Property Subtype"]')
                if subtype_dropdown.count() == 0:
                    for frame in page.frames:
                        subtype_dropdown = frame.locator('div.slicer-dropdown-menu[aria-label="Property Subtype"]')
                        if subtype_dropdown.count() > 0:
                            break

                if subtype_dropdown.count() == 0:
                    print(f"[ERROR] Property Subtype dropdown не найден")
                    continue

                subtype_dropdown.first.click()
                page.wait_for_timeout(2000)

                dropdown_popup = None
                all_popups = page.locator('div.slicer-dropdown-popup.focused')
                if all_popups.count() == 0:
                    for frame in page.frames:
                        all_popups = frame.locator('div.slicer-dropdown-popup.focused')
                        if all_popups.count() > 0:
                            break

                for i in range(all_popups.count()):
                    popup = all_popups.nth(i)
                    display_style = popup.evaluate('el => window.getComputedStyle(el).display')
                    if display_style == 'block':
                        dropdown_popup = popup
                        break

                scroll_region = dropdown_popup.locator('div.scrollRegion') if dropdown_popup else page.locator('div.scrollRegion')
                if scroll_region.count() == 0:
                    for frame in page.frames:
                        scroll_region = frame.locator('div.scrollRegion')
                        if scroll_region.count() > 0:
                            break

                all_rows = scroll_region.locator('div.row')
                type_clicked = False
                for i in range(all_rows.count()):
                    row = all_rows.nth(i)
                    slicer_item = row.locator('div.slicerItemContainer')
                    if slicer_item.count() > 0:
                        title = slicer_item.first.get_attribute('title')
                        if title == prop_type:
                            print(f"[>>] Кликаю по типу: {prop_type}")
                            slicer_item.first.click()
                            page.wait_for_timeout(2000)
                            type_clicked = True
                            break

                if not type_clicked:
                    print(f"[ERROR] Не удалось кликнуть по типу: {prop_type}")
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(2000)
                    continue

                page.keyboard.press("Escape")
                page.wait_for_timeout(2000)

                try:
                    page.wait_for_load_state("networkidle", timeout=2000)
                except:
                    pass
                page.wait_for_timeout(2000)

                print(f"\n[EXPORT] Sales Price Trend для {city} - {prop_type}")

                sales_file = download_property_table(page, city, prop_type, "Sales Price Trend", "sales")
                if sales_file:
                    downloaded_files.append(sales_file)

                print(f"\n[EXPORT] Rent Price Trend для {city} - {prop_type}")

                rent_file = download_property_table(page, city, prop_type, "Rent Price Trend", "rent")
                if rent_file:
                    downloaded_files.append(rent_file)

        context.close()

        print("\n" + "="*70)
        print("[CONVERT] ЗАПУСК КОНВЕРТАЦИИ ВСЕХ ФАЙЛОВ")
        print("="*70)

        import subprocess
        import sys

        convert_script = os.path.join(os.getcwd(), "convert_property_data.py")
        python_exe = sys.executable

        for filepath in downloaded_files:
            print(f"\n[>>] Конвертирую: {os.path.basename(filepath)}")
            try:
                result = subprocess.run(
                    [python_exe, convert_script],
                    env={**os.environ, "XLSX_FILE": filepath},
                    capture_output=False,
                    text=True
                )

                if result.returncode == 0:
                    print(f"[OK] Конвертация завершена")
                else:
                    print(f"[ERROR] Конвертация завершилась с ошибкой (код: {result.returncode})")
            except Exception as e:
                print(f"[ERROR] Ошибка при запуске конвертации: {e}")

        print("\n" + "="*70)
        print("[MERGE] ОБЪЕДИНЕНИЕ ВСЕХ JSON ФАЙЛОВ")
        print("="*70)

        merge_script = os.path.join(os.getcwd(), "merge_property_data.py")

        try:
            result = subprocess.run(
                [python_exe, merge_script],
                capture_output=False,
                text=True
            )

            if result.returncode == 0:
                print("\n[OK] Объединение завершено успешно")
            else:
                print(f"\n[ERROR] Объединение завершилось с ошибкой (код: {result.returncode})")
        except Exception as e:
            print(f"[ERROR] Ошибка при запуске объединения: {e}")

        print("\n[FINISH] Парсер завершен успешно")
        print(f"[INFO] Всего скачано файлов: {len(downloaded_files)}")
        print(f"[INFO] Итоговые файлы: sales_property_data.json, rent_property_data.json")

def download_property_table(page, city, prop_type, table_name, table_type):

    print(f"[>>] Ищу визуализацию '{table_name}'...")
    table_viz = page.locator(f'div[title="{table_name}"]')

    if table_viz.count() == 0:
        print("[INFO] Ищу в фреймах...")
        for frame in page.frames:
            table_viz = frame.locator(f'div[title="{table_name}"]')
            if table_viz.count() > 0:
                print(f"[OK] Найден в фрейме: {frame.name}")
                break

    if table_viz.count() == 0:
        print(f"[ERROR] '{table_name}' не найден")
        return None

    print(f"[>>] Кликаю по '{table_name}'...")
    table_viz.first.click()
    print("[OK] Клик выполнен")

    print("[INFO] Ожидание появления кнопок (1 сек)...")
    page.wait_for_timeout(2000)

    print("[>>] Ищу кнопку 'Дополнительные параметры'...")
    more_options = page.locator('button[aria-label="Дополнительные параметры"], button[aria-label="More options"]')

    if more_options.count() == 0:
        print("[INFO] Ищу в фреймах...")
        for frame in page.frames:
            more_options = frame.locator('button[aria-label="Дополнительные параметры"], button[aria-label="More options"]')
            if more_options.count() > 0:
                print(f"[OK] Найден в фрейме: {frame.name}")
                break

    if more_options.count() == 0:
        print("[ERROR] Кнопка 'Дополнительные параметры' не найдена")
        return None

    print("[>>] Кликаю 'Дополнительные параметры'...")
    more_options.first.click()
    print("[OK] Клик выполнен")

    page.wait_for_timeout(2000)

    print("[>>] Ищу кнопку 'Экспортировать данные'...")
    export_data = page.locator('button[title="Экспортировать данные"], button[title="Export data"]')

    if export_data.count() == 0:
        print("[INFO] Ищу в фреймах...")
        for frame in page.frames:
            export_data = frame.locator('button[title="Экспортировать данные"], button[title="Export data"]')
            if export_data.count() > 0:
                print(f"[OK] Найден в фрейме: {frame.name}")
                break

    if export_data.count() == 0:
        print("[ERROR] Кнопка 'Экспортировать данные' не найдена")
        return None

    print("[>>] Кликаю 'Экспортировать данные'...")
    export_data.first.click()
    print("[OK] Клик выполнен")

    page.wait_for_timeout(2000)

    print("[>>] Ищу кнопку 'Экспортировать'...")
    export_button = page.locator('button[aria-label="Экспортировать"], button[aria-label="Export"]')

    if export_button.count() == 0:
        print("[INFO] Ищу в фреймах...")
        for frame in page.frames:
            export_button = frame.locator('button[aria-label="Экспортировать"], button[aria-label="Export"]')
            if export_button.count() > 0:
                print(f"[OK] Найден в фрейме: {frame.name}")
                break

    if export_button.count() == 0:
        print("[ERROR] Кнопка 'Экспортировать' не найдена")
        return None

    print("[>>] Кликаю 'Экспортировать' и ожидаю скачивания...")

    with page.expect_download(timeout=120000) as download_info:
        export_button.first.click()

    download = download_info.value

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    city_safe = city.replace(" ", "_")
    prop_type_safe = prop_type.replace(" ", "_").replace("/", "_")
    filename = f"{city_safe}_{prop_type_safe}_{table_type}_property_data_{timestamp}.xlsx"
    filepath = os.path.join(os.getcwd(), filename)

    download.save_as(filepath)
    print(f"[OK] Файл скачан: {filename}")

    return filepath

if __name__ == "__main__":
    main()
