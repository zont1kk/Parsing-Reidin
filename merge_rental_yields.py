import json
import os
from collections import defaultdict

def merge_rental_yields_jsons():

    print("[START] Объединение JSON файлов rental yields")

    all_files = [f for f in os.listdir('.') if f.endswith('.json')]

    rental_yields_files = [
        f for f in all_files
        if '_rental_yields_data_' in f
        and f != 'rental_yields_data.json'
    ]

    print(f"[INFO] Найдено файлов для объединения: {len(rental_yields_files)}")

    if len(rental_yields_files) == 0:
        print("[WARNING] Нет файлов для объединения")
        return

    # Структура: {город: {тип: {дата: {локация: {bedroom: value}}}}}
    rental_yields_data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(dict))))

    total_count = 0

    for filename in rental_yields_files:
        print(f"\n[>>] Обработка: {filename}")

        # Парсим имя файла: City_Type_Bedroom_rental_yields_data_timestamp.json
        temp = filename.replace('_rental_yields_data_', '|')
        parts_before = temp.split('|')[0]

        # Разбиваем на части
        parts = parts_before.split('_')
        
        city = None
        property_type = None
        bedroom_key = None

        # Пытаемся найти город
        if len(parts) >= 3:
            # Проверяем двухсловные города
            if parts[0] == 'Abu' and parts[1] == 'Dhabi':
                city = 'Abu Dhabi'
                remaining_parts = parts[2:]
            elif parts[0] == 'Ras' and parts[1] == 'Al' and parts[2] == 'Khaimah':
                city = 'Ras Al Khaimah'
                remaining_parts = parts[3:]
            elif parts[0] == 'Umm' and parts[1] == 'Al' and parts[2] == 'Quwain':
                city = 'Umm Al Quwain'
                remaining_parts = parts[3:]
            # Односложные города
            elif parts[0] in ['Ajman', 'Dubai', 'Sharjah', 'Fujairah']:
                city = parts[0]
                remaining_parts = parts[1:]
            else:
                print(f"[ERROR] Не удалось определить город из: {parts_before}")
                continue

            # Теперь парсим property_type и bedroom
            # Формат: Apartment_0_(Studio) или Villa_3_Bedrooms или Apartment__All_Bedrooms
            if len(remaining_parts) >= 2:
                property_type = remaining_parts[0]  # Apartment или Villa
                
                # Определяем bedroom_key из оставшихся частей
                bedroom_parts = remaining_parts[1:]
                
                # Проверяем паттерны
                if bedroom_parts[0] == '' and len(bedroom_parts) >= 2 and bedroom_parts[1] == 'All':
                    # _All_Bedrooms → "all"
                    bedroom_key = "all"
                elif bedroom_parts[0] == '0' and len(bedroom_parts) >= 2:
                    # 0_(Studio) → "0"
                    bedroom_key = "0"
                elif bedroom_parts[0].isdigit():
                    # 1_Bedroom или 2_Bedrooms → "1", "2"
                    bedroom_key = bedroom_parts[0]
                else:
                    print(f"[ERROR] Не удалось определить bedroom из: {bedroom_parts}")
                    continue
            else:
                print(f"[ERROR] Недостаточно частей в: {remaining_parts}")
                continue

        else:
            print(f"[ERROR] Неверный формат имени файла: {filename}")
            continue

        print(f"[INFO] Город: {city}, Тип: {property_type}, Bedroom: {bedroom_key}")

        try:
            with open(filename, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
        except Exception as e:
            print(f"[ERROR] Ошибка чтения файла: {e}")
            continue

        if not file_data:
            print(f"[WARNING] Файл пустой, пропускаю")

            # try:
            #     os.remove(filename)
            #     print(f"[OK] Удален пустой файл: {filename}")
            # except Exception as e:
            #     print(f"[WARNING] Не удалось удалить файл {filename}: {e}")
            continue

        record_count = 0
        # file_data: {date: {location: {rental_yields_percent: value}}}
        # Новая структура: {город: {тип: {дата: {локация: {bedroom: value}}}}}
        for date, locations in file_data.items():
            for location, metrics in locations.items():
                # Получаем значение rental_yields_percent
                value = metrics.get('rental_yields_percent')
                if value is not None:
                    # Инициализируем структуру если нужно
                    if date not in rental_yields_data[city][property_type]:
                        rental_yields_data[city][property_type][date] = {}
                    if location not in rental_yields_data[city][property_type][date]:
                        rental_yields_data[city][property_type][date][location] = {}
                    
                    # Добавляем значение по ключу bedroom
                    rental_yields_data[city][property_type][date][location][bedroom_key] = value
                    record_count += 1

        print(f"[OK] Добавлено записей: {record_count}")
        total_count += record_count

        # try:
        #     os.remove(filename)
        #     print(f"[OK] Удален исходный файл: {filename}")
        # except Exception as e:
        #     print(f"[WARNING] Не удалось удалить файл {filename}: {e}")

    print("\n" + "="*70)
    print("[>>] Сохранение rental_yields_data.json...")

    # Функция для сортировки дат в формате DD.MM.YYYY
    from datetime import datetime
    def sort_date_key(date_str):
        try:
            return datetime.strptime(date_str, '%d.%m.%Y')
        except:
            return datetime.min

    # Конвертируем defaultdict в обычный dict для JSON с сортировкой дат
    rental_yields_final = {}
    for city, types in rental_yields_data.items():
        rental_yields_final[city] = {}
        for prop_type, dates in types.items():
            rental_yields_final[city][prop_type] = {}
            # Сортируем даты
            sorted_dates = sorted(dates.keys(), key=sort_date_key)
            for date in sorted_dates:
                locations = dates[date]
                rental_yields_final[city][prop_type][date] = {
                    location: dict(bedrooms)
                    for location, bedrooms in locations.items()
                }

    with open('rental_yields_data.json', 'w', encoding='utf-8') as f:
        json.dump(rental_yields_final, f, ensure_ascii=False, indent=2)

    print(f"[OK] Сохранено городов: {len(rental_yields_final)}")
    print(f"[OK] Всего записей: {total_count}")

    # Статистика по структуре: город → тип → дата → локация → bedrooms
    for city, types in rental_yields_final.items():
        for prop_type, dates in types.items():
            total_records = sum(
                len(locations) 
                for locations in dates.values()
            )
            print(f"  - {city} / {prop_type}: {total_records} date-location комбинаций")

    print("\n" + "="*70)
    print("[SUCCESS] Объединение завершено успешно")
    print(f"[INFO] Создан файл: rental_yields_data.json")

if __name__ == "__main__":
    merge_rental_yields_jsons()
