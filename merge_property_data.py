import json
import os
from collections import defaultdict

def merge_property_data_jsons():

    print("[START] Объединение JSON файлов property data")

    all_files = [f for f in os.listdir('.') if f.endswith('.json')]

    property_files = [
        f for f in all_files
        if '_property_data_' in f
        and f not in ['sales_property_data.json', 'rent_property_data.json']
    ]

    print(f"[INFO] Найдено файлов для объединения: {len(property_files)}")

    if len(property_files) == 0:
        print("[WARNING] Нет файлов для объединения")
        return

    sales_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    rent_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    sales_count = 0
    rent_count = 0

    for filename in property_files:
        print(f"\n[>>] Обработка: {filename}")

        parts = filename.replace('.json', '').split('_')

        if '_sales_property_data_' in filename:
            data_type = 'sales'

            sales_idx = None
            for i, part in enumerate(parts):
                if part == 'sales':
                    sales_idx = i
                    break

            if sales_idx is None:
                print(f"[ERROR] Не удалось определить структуру имени: {filename}")
                continue

            city_parts = parts[:sales_idx]

            temp = filename.replace('_sales_property_data_', '|')
            city_and_type = temp.split('|')[0]

            known_cities = ['Abu_Dhabi', 'Ajman', 'Dubai', 'Ras_Al_Khaimah', 'Sharjah', 'Umm_Al_Quwain', 'Fujairah']

            city = None
            property_type = None

            for known_city in known_cities:
                if city_and_type.startswith(known_city + '_'):
                    city = known_city.replace('_', ' ')
                    property_type = city_and_type[len(known_city) + 1:].replace('_', ' ')
                    break
                elif city_and_type == known_city:
                    city = known_city.replace('_', ' ')
                    property_type = ''
                    break

            if not city:
                print(f"[ERROR] Не удалось определить город из: {city_and_type}")
                continue

        elif '_rent_property_data_' in filename:
            data_type = 'rent'
            temp = filename.replace('_rent_property_data_', '|')
            city_and_type = temp.split('|')[0]

            known_cities = ['Abu_Dhabi', 'Ajman', 'Dubai', 'Ras_Al_Khaimah', 'Sharjah', 'Umm_Al_Quwain', 'Fujairah']

            city = None
            property_type = None

            for known_city in known_cities:
                if city_and_type.startswith(known_city + '_'):
                    city = known_city.replace('_', ' ')
                    property_type = city_and_type[len(known_city) + 1:].replace('_', ' ')
                    break
                elif city_and_type == known_city:
                    city = known_city.replace('_', ' ')
                    property_type = ''
                    break

            if not city:
                print(f"[ERROR] Не удалось определить город из: {city_and_type}")
                continue
        else:
            print(f"[WARNING] Пропускаю файл (не sales/rent): {filename}")
            continue

        print(f"[INFO] Город: {city}, Тип: {property_type}, Данные: {data_type}")

        try:
            with open(filename, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
        except Exception as e:
            print(f"[ERROR] Ошибка чтения файла: {e}")
            continue

        if not file_data:
            print(f"[WARNING] Файл пустой, пропускаю")
            continue

        record_count = 0
        for date, properties in file_data.items():
            for property_name, metrics in properties.items():
                if data_type == 'sales':
                    sales_data[city][property_type][date][property_name] = metrics
                else:
                    rent_data[city][property_type][date][property_name] = metrics
                record_count += 1

        print(f"[OK] Добавлено записей: {record_count}")

        if data_type == 'sales':
            sales_count += record_count
        else:
            rent_count += record_count

        try:
            os.remove(filename)
            print(f"[OK] Удален исходный файл: {filename}")
        except Exception as e:
            print(f"[WARNING] Не удалось удалить файл {filename}: {e}")

    print("\n" + "="*70)
    print("[>>] Сохранение sales_property_data.json...")

    sales_final = {
        city: {
            prop_type: dict(dates)
            for prop_type, dates in types.items()
        }
        for city, types in sales_data.items()
    }

    with open('sales_property_data.json', 'w', encoding='utf-8') as f:
        json.dump(sales_final, f, ensure_ascii=False, indent=2)

    print(f"[OK] Сохранено городов: {len(sales_final)}")
    print(f"[OK] Всего записей: {sales_count}")

    for city, types in sales_final.items():
        type_counts = {prop_type: sum(len(props) for props in dates.values()) for prop_type, dates in types.items()}
        print(f"  - {city}: {type_counts}")

    print("\n" + "="*70)
    print("[>>] Сохранение rent_property_data.json...")

    rent_final = {
        city: {
            prop_type: dict(dates)
            for prop_type, dates in types.items()
        }
        for city, types in rent_data.items()
    }

    with open('rent_property_data.json', 'w', encoding='utf-8') as f:
        json.dump(rent_final, f, ensure_ascii=False, indent=2)

    print(f"[OK] Сохранено городов: {len(rent_final)}")
    print(f"[OK] Всего записей: {rent_count}")

    for city, types in rent_final.items():
        type_counts = {prop_type: sum(len(props) for props in dates.values()) for prop_type, dates in types.items()}
        print(f"  - {city}: {type_counts}")

    print("\n" + "="*70)
    print("[SUCCESS] Объединение завершено успешно")
    print(f"[INFO] Создано файлов: sales_property_data.json, rent_property_data.json")

if __name__ == "__main__":
    merge_property_data_jsons()
