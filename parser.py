from playwright.sync_api import sync_playwright
import json
from datetime import datetime, timedelta
from collections import defaultdict
import os
import subprocess
from typing import Any, Dict, List, Optional, Tuple

def load_config() -> Dict[str, Any]:
    config_file = os.path.join(os.getcwd(), "config.json")
    if not os.path.exists(config_file):
        print("[ERROR] config.json не найден")
        return {}
    with open(config_file, "r", encoding="utf-8") as f:
        config: Dict[str, Any] = json.load(f)
    return config

config = load_config()
auth_config = config["auth"]
DEVICE_ID = auth_config["device_id"]
USERNAME = auth_config["username"]
PASSWORD = auth_config["password"]

def load_areas() -> List[str]:
    if not os.path.exists("areas.txt"):
        print("[ERROR] Файл areas.txt не найден")
        return []
    with open("areas.txt", "r", encoding="utf-8") as f:
        areas = [line.strip() for line in f if line.strip()]
    print(f"[OK] Загружено {len(areas)} районов")
    return areas

def get_default_area() -> str:
    """Получает дефолтный район из конфига или возвращает Business Bay"""
    return config.get("default_area", "Business Bay")

def create_handle_response(area_name: str, captured_requests: List[Dict[str, Any]], page: Any) -> Any:
    """Фабрика для создания обработчика ответов для конкретного района"""
    def handle_response(response: Any) -> None:
        url = response.url
        if "/query" in url:
            try:
                request_data = response.request.post_data
                response_data = response.json()
                if not request_data:
                    return
                
                request_json = json.loads(request_data)
                
                captured_requests.append({
                    "request": request_json,
                    "response": response_data
                })
                
                print(f"[API {area_name}] Запрос перехвачен (всего: {len(captured_requests)})")
            except Exception as e:
                print(f"[ERROR] {url}: {e}")
    return handle_response

def main() -> None:
    areas = load_areas()
    if not areas:
        return
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context()

        page = context.new_page()

        page.goto("https://insight.reidin.com/", wait_until="load", timeout=60000)

        page.evaluate(f"localStorage.setItem('deviceId', '{DEVICE_ID}');")
        print("[OK] device_id установлен")

        page.goto("https://insight.reidin.com/auth/login", wait_until="load", timeout=60000)
        print("[OK] Страница логина открыта")

        page.fill('#input-emaillogin-desktop', USERNAME)
        page.fill('#input-passwordlogin-desktop', PASSWORD)

        page.locator('xpath=//input[@id="input-emaillogin-desktop"]/ancestor::form[1]//button[@type="submit"]').click()

        page.wait_for_load_state("networkidle", timeout=60000)

        context.storage_state(path="state.json")
        print("[OK] Авторизация успешна. state.json сохранён.")

        page.goto("https://insight.reidin.com/home/dashboard/754", wait_until="load", timeout=60000)
        print("[OK] Dashboard открыт")

        # === ПЕРЕХВАТ ДАННЫХ ДЕФОЛТНОГО РАЙОНА (он уже загружен) ===
        default_area = get_default_area()
        print(f"[INFO] Перехватываю данные дефолтного района: {default_area}")
        
        default_base_requests = []
        default_handler = create_handle_response(default_area, default_base_requests, page)
        page.on("response", default_handler)
        
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except:
            pass
        print(f"[OK] Dashboard загружен. Перехвачено {len(default_base_requests)} запросов для {default_area}")

        page.remove_listener("response", default_handler)
        
        print("[INFO] Ожидание полной загрузки страницы (10 сек)...")
        page.wait_for_timeout(10000)

        start_date_str = os.environ.get("PARSER_START_DATE")
        end_date_str = os.environ.get("PARSER_END_DATE")
        everyday_str = os.environ.get("PARSER_EVERYDAY").lower()
        everyday = everyday_str == "true"
        if start_date_str and end_date_str:
            print(f"[INFO] Даты из конфига: {start_date_str} - {end_date_str} (everyday: {everyday})")
            start_date = datetime.strptime(start_date_str, "%d.%m.%Y")
            end_date = datetime.strptime(end_date_str, "%d.%m.%Y")
        else:
            print(f"[INFO] Даты из окружения не найдены, использую default: -10 до -4 дней")
            start_date = datetime.now() - timedelta(days=10)
            end_date = datetime.now() - timedelta(days=4)
            everyday = True
        dates_to_process = []
        if everyday:
            current_date = start_date
            while current_date <= end_date:
                dates_to_process.append(current_date.strftime("%d.%m.%Y"))
                current_date += timedelta(days=1)
            print(f"[INFO] Режим EVERYDAY: буду обрабатывать {len(dates_to_process)} дат")
        else:
            dates_to_process = [
                (start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y"))
            ]
            print(f"[INFO] Режим RANGE: один снимок от {start_date_str} до {end_date_str}")
        print(f"[INFO] Даты обработки: {dates_to_process}")
        output_raw_file = os.environ.get("PARSER_OUTPUT_RAW_FILE")
        output_file = os.path.join(os.getcwd(), output_raw_file)
        if os.path.exists(output_file):
            print(f"[INFO] Загружаю существующие данные из {output_file}...")
            with open(output_file, "r", encoding="utf-8") as f:
                all_dates_result = json.load(f)
            print(f"[OK] Загружено {sum(len(areas) for areas in all_dates_result.values())} районов")
        else:
            print(f"[INFO] Создаю новый файл {output_file}...")
            all_dates_result = {}
        if everyday:
            for date_key in dates_to_process:
                if date_key not in all_dates_result:
                    all_dates_result[date_key] = {}
        else:
            date_range_key = f"{dates_to_process[0][0]}-{dates_to_process[0][1]}"
            if date_range_key not in all_dates_result:
                all_dates_result[date_range_key] = {}
        def process_area_day(area_name, date_pair, is_first_day, base_captured_requests):
            """Обрабатывает один день (или диапазон) для одного района"""
            if isinstance(date_pair, tuple):
                date_start_str, date_end_str = date_pair
                date_display = f"{date_start_str} - {date_end_str}"
            else:
                date_start_str = date_end_str = date_pair
                date_display = date_pair
            
            print(f"\n    [DATE] {area_name}: {date_display} (день {'1' if is_first_day else '2+'})")
            
            captured_requests = []
            response_handler = None
            
            try:
                date_start_input = page.locator('input[aria-label^="Дата начала"]')
                if date_start_input.count() == 0:
                    for frame in page.frames:
                        date_start_input = frame.locator('input[aria-label^="Дата начала"]')
                        if date_start_input.count() > 0:
                            break
                date_end_input = page.locator('input[aria-label^="Дата окончания"]')
                if date_end_input.count() == 0:
                    for frame in page.frames:
                        date_end_input = frame.locator('input[aria-label^="Дата окончания"]')
                        if date_end_input.count() > 0:
                            break
                if is_first_day:
                    if date_start_input.count() > 0:
                        date_start_input.first.clear()
                        date_start_input.first.fill(date_start_str)
                        page.wait_for_timeout(500)
                        print(f"      [OK] Дата начала: {date_start_str}")
                    try:
                        page.wait_for_load_state("networkidle", timeout=2000)
                    except:
                        pass
                    
                    print(f"      [INFO] Подключаю обработчик для перехвата запросов...")
                    response_handler = create_handle_response(area_name, captured_requests, page)
                    page.on("response", response_handler)
                    
                    if date_end_input.count() > 0:
                        date_end_input.first.clear()
                        date_end_input.first.fill(date_end_str)
                        page.wait_for_timeout(500)
                        print(f"      [OK] Дата окончания: {date_end_str}")
                else:
                    if date_end_input.count() > 0:
                        date_end_input.first.clear()
                        date_end_input.first.fill(date_end_str)
                        page.wait_for_timeout(500)
                        print(f"      [OK] Дата окончания: {date_end_str}")
                    try:
                        page.wait_for_load_state("networkidle", timeout=2000)
                    except:
                        pass
                    
                    print(f"      [INFO] Подключаю обработчик для перехвата запросов...")
                    response_handler = create_handle_response(area_name, captured_requests, page)
                    page.on("response", response_handler)
                    
                    if date_start_input.count() > 0:
                        date_start_input.first.clear()
                        date_start_input.first.fill(date_start_str)
                        page.wait_for_timeout(500)
                        print(f"      [OK] Дата начала: {date_start_str}")
                try:
                    page.wait_for_load_state("networkidle", timeout=3000)
                except:
                    pass
                page.wait_for_timeout(2000)
            except Exception as e:
                print(f"      [ERROR] Ошибка при установке дат: {e}")
            if response_handler:
                page.remove_listener("response", response_handler)
            
            if isinstance(date_pair, tuple):
                date_key = f"{date_pair[0]}-{date_pair[1]}"
            else:
                date_key = date_pair
            
            new_requests = captured_requests.copy()
            
            def get_request_key(req):
                """Создаёт уникальный ключ для запроса из SELECT + WHERE условий"""
                queries = req.get('request', {}).get('queries', [])
                if not queries:
                    return None
                
                query = queries[0]
                cmd = query.get('Query', {}).get('Commands', [{}])[0]
                sq = cmd.get('SemanticQueryDataShapeCommand', {}).get('Query', {})
                
                select_list = sq.get('Select', [])
                select_name = select_list[0].get('Name', '') if select_list else ''
                
                where_parts = []
                for where in sq.get('Where', []):
                    cond = where.get('Condition', {})
                    if 'In' in cond:
                        prop = cond['In']['Expressions'][0].get('Column', {}).get('Property', '')
                        vals = cond['In'].get('Values', [])
                        clean_vals = []
                        for val_group in vals:
                            for val_item in val_group:
                                if 'Literal' in val_item:
                                    clean_vals.append(val_item['Literal']['Value'].strip("'"))
                        where_parts.append(f"{prop}={'|'.join(sorted(clean_vals))}")
                
                key = select_name + '::' + '::'.join(sorted(where_parts))
                return key
            
            if date_key in all_dates_result and area_name in all_dates_result[date_key]:
                existing_requests = all_dates_result[date_key][area_name]
                
                existing_by_key = {}
                for req in existing_requests:
                    key = get_request_key(req)
                    if key:
                        existing_by_key[key] = req
                
                updated_count = 0
                added_count = 0
                for new_req in new_requests:
                    key = get_request_key(new_req)
                    if key:
                        if key in existing_by_key:
                            existing_by_key[key] = new_req
                            updated_count += 1
                        else:
                            existing_by_key[key] = new_req
                            added_count += 1
                
                all_requests = list(existing_by_key.values())
                print(f"      [OK] {area_name} завершен (обновлено: {updated_count}, добавлено: {added_count}, итого: {len(all_requests)})")
            else:
                all_requests = new_requests
                print(f"      [OK] {area_name} завершен (новых: {len(all_requests)})")
            
            all_dates_result[date_key][area_name] = all_requests
            
            return all_requests
        def set_date_to_today():
            year_ago = (datetime.now() - timedelta(days=365)).strftime("%d.%m.%Y")
            today_str = datetime.now().strftime("%d.%m.%Y")
            print(f"  [INFO] Устанавливаю диапазон дат: {year_ago} - {today_str}")
            try:
                date_start_input = page.locator('input[aria-label^="Дата начала"]')
                if date_start_input.count() == 0:
                    for frame in page.frames:
                        date_start_input = frame.locator('input[aria-label^="Дата начала"]')
                        if date_start_input.count() > 0:
                            break
                date_end_input = page.locator('input[aria-label^="Дата окончания"]')
                if date_end_input.count() == 0:
                    for frame in page.frames:
                        date_end_input = frame.locator('input[aria-label^="Дата окончания"]')
                        if date_end_input.count() > 0:
                            break

                if date_start_input.count() > 0:
                    date_start_input.first.clear()
                    date_start_input.first.fill(year_ago)
                    page.wait_for_timeout(300)

                if date_end_input.count() > 0:
                    date_end_input.first.clear()
                    date_end_input.first.fill(today_str)
                    page.wait_for_timeout(300)
                try:
                    page.wait_for_load_state("networkidle", timeout=2000)
                except:
                    pass
            except Exception as e:
                print(f"  [ERROR] Ошибка при установке диапазона дат: {e}")
        
        # === СОХРАНЕНИЕ ДАННЫХ ДЕФОЛТНОГО РАЙОНА ===
        if dates_to_process:
            first_date = dates_to_process[0]
            if isinstance(first_date, tuple):
                date_key = f"{first_date[0]}-{first_date[1]}"
            else:
                date_key = first_date
            
            if date_key not in all_dates_result:
                all_dates_result[date_key] = {}
            all_dates_result[date_key][default_area] = default_base_requests.copy()
            print(f"[OK] Данные дефолтного района {default_area} сохранены")
        
        # === ОБРАБОТКА ДАТ ДЛЯ ДЕФОЛТНОГО РАЙОНА ===
        print(f"\n[INFO] Обработка различных дат для {default_area}...")
        for day_index, date_str in enumerate(dates_to_process):
            is_first_day = (day_index == 0)
            process_area_day(default_area, date_str, is_first_day, default_base_requests)
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_dates_result, f, ensure_ascii=False, indent=2)
        
        set_date_to_today()
        
        # === ОСНОВНОЙ ЦИКЛ: обработка остальных районов ===
        print(f"\n[INFO] Начинаю обработку остальных районов...")
        areas_to_process = [a for a in areas if a != default_area]
        
        if not areas_to_process:
            print(f"[INFO] Все районы обработаны (только дефолтный район в списке)")
        
        for area in areas_to_process:
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
                    # Шаг 1: Найти и нажать на expandButton для раскрытия подрайонов
                    expand_button = target_element.locator('div.expandButton')
                    if expand_button.count() > 0:
                        print(f"  [INFO] Раскрываю подрайоны...")
                        expand_button.first.click()
                        page.wait_for_timeout(3000)
                    
                    # Шаг 2: Собрать все подрайоны (aria-level="2")
                    all_rows = scroll_region.locator('div.row')
                    subareas = []
                    for i in range(all_rows.count()):
                        row = all_rows.nth(i)
                        try:
                            # Ищем slicerItemContainer внутри row
                            slicer_item = row.locator('div.slicerItemContainer').first
                            if slicer_item.count() > 0:
                                aria_level = slicer_item.get_attribute('aria-level')
                                if aria_level == '2':
                                    subarea_title = slicer_item.get_attribute('title')
                                    if subarea_title:
                                        subareas.append(subarea_title)
                        except:
                            pass
                    
                    print(f"  [INFO] Найдено подрайонов: {len(subareas)} ({', '.join(subareas[:5])}{'...' if len(subareas) > 5 else ''})")
                    
                    # Шаг 3: Собрать данные главного района
                    base_captured_requests = []
                    base_handler = create_handle_response(area, base_captured_requests, page)
                    page.on("response", base_handler)
                    
                    print(f"  [INFO] Подключен обработчик для перехвата базовых метрик главного района...")
                    target_element.click()
                    page.wait_for_timeout(3000)
                    
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                    except:
                        pass
                    
                    page.remove_listener("response", base_handler)
                    print(f"  [OK] Базовые метрики главного района перехвачены: {len(base_captured_requests)} запросов")
                    
                    # Сохранение базовых данных главного района
                    if dates_to_process:
                        first_date = dates_to_process[0]
                        if isinstance(first_date, tuple):
                            date_key = f"{first_date[0]}-{first_date[1]}"
                        else:
                            date_key = first_date
                        
                        if date_key not in all_dates_result:
                            all_dates_result[date_key] = {}
                        all_dates_result[date_key][area] = base_captured_requests.copy()
                    
                    # Обработка дат для главного района
                    for day_index, date_str in enumerate(dates_to_process):
                        is_first_day = (day_index == 0)
                        process_area_day(area, date_str, is_first_day, base_captured_requests)
                        with open(output_file, "w", encoding="utf-8") as f:
                            json.dump(all_dates_result, f, ensure_ascii=False, indent=2)

                    set_date_to_today()
                    
                    # Шаг 4: Обработка каждого подрайона
                    for subarea in subareas:
                        print(f"\n  {'='*50}")
                        print(f"  [SUBAREA] {area} -> {subarea}")
                        print(f"  {'='*50}")
                        
                        # Открыть dropdown снова
                        dropdown_menu.first.click()
                        page.wait_for_timeout(2000)
                        
                        # Поиск подрайона
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
                            for char in subarea:
                                search_input.first.type(char)
                                page.wait_for_timeout(80)
                            page.wait_for_timeout(2000)
                        
                        # Найти элемент подрайона
                        scroll_region_sub = page.locator('div.scrollRegion')
                        if scroll_region_sub.count() == 0:
                            for frame in page.frames:
                                scroll_region_sub = frame.locator('div.scrollRegion')
                                if scroll_region_sub.count() > 0:
                                    break
                        
                        all_rows_sub = scroll_region_sub.locator('div.row')
                        subarea_element = None
                        
                        for i in range(all_rows_sub.count()):
                            row = all_rows_sub.nth(i)
                            # Ищем slicerItemContainer с aria-level="2" и нужным title
                            slicer_items = row.locator('div.slicerItemContainer')
                            for j in range(slicer_items.count()):
                                slicer_item = slicer_items.nth(j)
                                try:
                                    aria_level = slicer_item.get_attribute('aria-level')
                                    item_title = slicer_item.get_attribute('title')
                                    if aria_level == '2' and item_title == subarea:
                                        subarea_element = slicer_item
                                        break
                                except:
                                    pass
                            if subarea_element:
                                break
                        
                        if subarea_element:
                            # Перехват данных подрайона
                            subarea_captured_requests = []
                            subarea_handler = create_handle_response(f"{area} - {subarea}", subarea_captured_requests, page)
                            page.on("response", subarea_handler)
                            
                            print(f"    [INFO] Подключен обработчик для подрайона...")
                            subarea_element.click()
                            page.wait_for_timeout(3000)
                            
                            try:
                                page.wait_for_load_state("networkidle", timeout=5000)
                            except:
                                pass
                            
                            page.remove_listener("response", subarea_handler)
                            print(f"    [OK] Данные подрайона перехвачены: {len(subarea_captured_requests)} запросов")
                            
                            # Сохранение базовых данных подрайона
                            subarea_key = f"{area} - {subarea}"
                            if dates_to_process:
                                first_date = dates_to_process[0]
                                if isinstance(first_date, tuple):
                                    date_key = f"{first_date[0]}-{first_date[1]}"
                                else:
                                    date_key = first_date
                                
                                if date_key not in all_dates_result:
                                    all_dates_result[date_key] = {}
                                all_dates_result[date_key][subarea_key] = subarea_captured_requests.copy()
                            
                            # Обработка дат для подрайона
                            for day_index, date_str in enumerate(dates_to_process):
                                is_first_day = (day_index == 0)
                                process_area_day(subarea_key, date_str, is_first_day, subarea_captured_requests)
                                with open(output_file, "w", encoding="utf-8") as f:
                                    json.dump(all_dates_result, f, ensure_ascii=False, indent=2)
                            
                            set_date_to_today()
                        else:
                            print(f"    [WARNING] Не найден элемент для подрайона: {subarea}")
                else:
                    print(f"[WARNING] Не найден элемент для района: {area}")
            else:
                print(f"[WARNING] Не найден инпут поиска для района: {area}")
        browser.close()
        print(f"\n[INFO] Финальное сохранение данных батча...")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_dates_result, f, ensure_ascii=False, indent=2)
        total_queries = sum(sum(len(metrics) for metrics in areas_data.values())
                           for areas_data in all_dates_result.values())
        print(f"[OK] {output_raw_file} обновлен ({total_queries} всего запросов)")
        print(f"[OK] Батч завершен. Трансформация будет выполнена после всех батчей.")

if __name__ == "__main__":
    main()
