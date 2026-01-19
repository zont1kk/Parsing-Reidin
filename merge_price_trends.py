import json
import os
from collections import defaultdict

def merge_price_trends():

    sales_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    rent_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    json_files = []
    for f in os.listdir('.'):
        if f.endswith('.json') and 'price_trend' in f:

            if f not in ['sales_price_trend.json', 'rent_price_trend.json']:

                if '_sales_price_trend_' in f or '_rent_price_trend_' in f:
                    json_files.append(f)

    print(f"[INFO] Найдено {len(json_files)} файлов для объединения")

    for filename in json_files:

        parts = filename.replace('.json', '').split('_')

        city_parts = []
        idx = 0
        while idx < len(parts):
            if parts[idx] in ['Apartment', 'Villa']:
                break
            city_parts.append(parts[idx])
            idx += 1

        city = ' '.join(city_parts)

        if idx < len(parts):
            prop_type = parts[idx]
            idx += 1
        else:
            print(f"[WARNING] Не удалось определить тип для {filename}")
            continue

        if idx < len(parts) and parts[idx] in ['sales', 'rent']:
            data_type = parts[idx]
        else:
            print(f"[WARNING] Не удалось определить тип данных для {filename}")
            continue

        print(f"[>>] Обрабатываю: {city} - {prop_type} - {data_type}")

        try:
            with open(filename, 'r', encoding='utf-8') as f:
                file_data = json.load(f)

            if data_type == 'sales':
                for date, locations in file_data.items():
                    for location, metrics in locations.items():
                        sales_data[city][prop_type][date][location] = metrics
            else:
                for date, locations in file_data.items():
                    for location, metrics in locations.items():
                        rent_data[city][prop_type][date][location] = metrics

            records_count = sum(len(locs) for locs in file_data.values())
            print(f"    [OK] Добавлено записей: {records_count}")

            try:
                os.remove(filename)
                print(f"    [OK] Удален исходный файл: {filename}")
            except Exception as e:
                print(f"    [WARNING] Не удалось удалить файл {filename}: {e}")

        except Exception as e:
            print(f"[ERROR] Ошибка при обработке {filename}: {e}")
            continue

    sales_final = {
        city: {
            prop_type: {
                date: dict(locations)
                for date, locations in dates.items()
            }
            for prop_type, dates in types.items()
        }
        for city, types in sales_data.items()
    }

    rent_final = {
        city: {
            prop_type: {
                date: dict(locations)
                for date, locations in dates.items()
            }
            for prop_type, dates in types.items()
        }
        for city, types in rent_data.items()
    }

    print("\n" + "="*70)
    print("[SAVE] Сохранение итоговых файлов...")
    print("="*70)

    with open('sales_price_trend.json', 'w', encoding='utf-8') as f:
        json.dump(sales_final, f, ensure_ascii=False, indent=2)

    sales_total = sum(
        sum(
            sum(len(locs) for locs in dates.values())
            for dates in types.values()
        )
        for types in sales_data.values()
    )
    print(f"[OK] sales_price_trend.json создан")
    print(f"    Городов: {len(sales_final)}")
    print(f"    Всего записей: {sales_total}")

    with open('rent_price_trend.json', 'w', encoding='utf-8') as f:
        json.dump(rent_final, f, ensure_ascii=False, indent=2)

    rent_total = sum(
        sum(
            sum(len(locs) for locs in dates.values())
            for dates in types.values()
        )
        for types in rent_data.values()
    )
    print(f"[OK] rent_price_trend.json создан")
    print(f"    Городов: {len(rent_final)}")
    print(f"    Всего записей: {rent_total}")

    print("\n" + "="*70)
    print("[EXAMPLE] Структура данных:")
    print("="*70)

    if sales_final:
        first_city = list(sales_final.keys())[0]
        first_type = list(sales_final[first_city].keys())[0]
        first_date = list(sales_final[first_city][first_type].keys())[0]
        first_location = list(sales_final[first_city][first_type][first_date].keys())[0]

        print(f"{first_city} -> {first_type} -> {first_date} -> {first_location}:")
        print(f"  {json.dumps(sales_final[first_city][first_type][first_date][first_location], indent=2, ensure_ascii=False)}")

    print("\n[FINISH] Объединение завершено успешно")

if __name__ == "__main__":
    merge_price_trends()
