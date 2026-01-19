import json
import os
from collections import defaultdict

def merge_yields_jsons():

    print("[START] Объединение JSON файлов yields")

    all_files = [f for f in os.listdir('.') if f.endswith('.json')]

    yields_files = [
        f for f in all_files
        if '_yields_data_' in f
        and f != 'yields_data.json'
    ]

    print(f"[INFO] Найдено файлов для объединения: {len(yields_files)}")

    if len(yields_files) == 0:
        print("[WARNING] Нет файлов для объединения")
        return

    yields_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    yields_count = 0

    for filename in yields_files:
        print(f"\n[>>] Обработка: {filename}")

        temp = filename.replace('_yields_data_', '|')
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

        print(f"[INFO] Город: {city}, Тип: {property_type}")

        try:
            with open(filename, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
        except Exception as e:
            print(f"[ERROR] Ошибка чтения файла: {e}")
            continue

        if not file_data:
            print(f"[WARNING] Файл пустой, пропускаю")

            try:
                os.remove(filename)
                print(f"[OK] Удален пустой файл: {filename}")
            except Exception as e:
                print(f"[WARNING] Не удалось удалить файл {filename}: {e}")
            continue

        record_count = 0
        for date, properties in file_data.items():
            for property_name, metrics in properties.items():
                yields_data[city][property_type][date][property_name] = metrics
                record_count += 1

        print(f"[OK] Добавлено записей: {record_count}")
        yields_count += record_count

        try:
            os.remove(filename)
            print(f"[OK] Удален исходный файл: {filename}")
        except Exception as e:
            print(f"[WARNING] Не удалось удалить файл {filename}: {e}")

    print("\n" + "="*70)
    print("[>>] Сохранение yields_data.json...")

    yields_final = {
        city: {
            prop_type: dict(dates)
            for prop_type, dates in types.items()
        }
        for city, types in yields_data.items()
    }

    with open('yields_data.json', 'w', encoding='utf-8') as f:
        json.dump(yields_final, f, ensure_ascii=False, indent=2)

    print(f"[OK] Сохранено городов: {len(yields_final)}")
    print(f"[OK] Всего записей: {yields_count}")

    for city, types in yields_final.items():
        type_counts = {prop_type: sum(len(props) for props in dates.values()) for prop_type, dates in types.items()}
        print(f"  - {city}: {type_counts}")

    print("\n" + "="*70)
    print("[SUCCESS] Объединение завершено успешно")
    print(f"[INFO] Создан файл: yields_data.json")

if __name__ == "__main__":
    merge_yields_jsons()
