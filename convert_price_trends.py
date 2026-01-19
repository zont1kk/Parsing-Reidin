import pandas as pd
import json
import os
from datetime import datetime
from collections import defaultdict

def convert_xlsx_to_json(xlsx_file, output_file=None):

    if not os.path.exists(xlsx_file):
        print(f"[ERROR] Файл {xlsx_file} не найден")
        return None

    print(f"[OK] Загружаю: {xlsx_file}")

    df = pd.read_excel(xlsx_file, skiprows=2)

    print(f"[OK] Загружено {len(df)} строк")
    print(f"[INFO] Колонки: {df.columns.tolist()}")

    result = defaultdict(lambda: defaultdict(dict))

    for idx, row in df.iterrows():
        try:

            date = pd.to_datetime(row['Date'])
            date_key = date.strftime('%d.%m.%Y')

            location = str(row['Location'])

            if 'Average Sales Price (AED/Sqf)' in df.columns:
                price_col = 'Average Sales Price (AED/Sqf)'
                price_key = 'average_sales_price'
            elif 'Average Rent Price (AED/Sqf/Annum)' in df.columns:
                price_col = 'Average Rent Price (AED/Sqf/Annum)'
                price_key = 'average_rent_price'
            else:

                price_cols = [col for col in df.columns if 'Price' in col and 'Change' not in col]
                if price_cols:
                    price_col = price_cols[0]
                    price_key = 'average_price'
                else:
                    print(f"[ERROR] Не найдена колонка с ценой")
                    return None

            avg_price = float(row[price_col])

            mom_change = row.get('M-o-m Change (%)')
            qoq_change = row.get('Q-o-q Change (%)')
            yoy_change = row.get('Y-o-y Change (%)')

            if pd.isna(mom_change):
                mom_change = None
            else:
                mom_change = round(float(mom_change) * 100, 2)

            if pd.isna(qoq_change):
                qoq_change = None
            else:
                qoq_change = round(float(qoq_change) * 100, 2)

            if pd.isna(yoy_change):
                yoy_change = None
            else:
                yoy_change = round(float(yoy_change) * 100, 2)

            result[date_key][location] = {
                price_key: round(avg_price, 2),
                'mom_change_percent': mom_change,
                'qoq_change_percent': qoq_change,
                'yoy_change_percent': yoy_change
            }

        except Exception as e:
            print(f"[WARNING] Ошибка в строке {idx}: {e}")
            continue

    final_result = {
        date: dict(locations)
        for date, locations in result.items()
    }

    if output_file is None:
        base_name = os.path.splitext(xlsx_file)[0]
        output_file = f"{base_name}.json"

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(final_result, f, ensure_ascii=False, indent=2)

    total_dates = len(final_result)
    total_locations = sum(len(locations) for locations in final_result.values())

    print(f'\n{"="*80}')
    print(f'[OK] {output_file} создан')
    print(f'[OK] Дат: {total_dates}')
    print(f'[OK] Всего записей (дата*район): {total_locations}')

    if final_result:
        first_date = list(final_result.keys())[0]
        first_location = list(final_result[first_date].keys())[0]
        print(f'\n[EXAMPLE] {first_date} -> {first_location}:')
        print(f'  {json.dumps(final_result[first_date][first_location], indent=2)}')

    print(f'{"="*80}\n')

    try:
        os.remove(xlsx_file)
        print(f"[OK] Удален исходный файл: {os.path.basename(xlsx_file)}")
    except Exception as e:
        print(f"[WARNING] Не удалось удалить файл {xlsx_file}: {e}")

    return final_result

if __name__ == '__main__':
    import sys

    xlsx_file = os.environ.get('XLSX_FILE')

    if xlsx_file and os.path.exists(xlsx_file):
        print(f"[INFO] Конвертирую файл: {xlsx_file}")
        convert_xlsx_to_json(xlsx_file)
    else:

        files = [f for f in os.listdir('.') if f.startswith('sales_price_trend_') and f.endswith('.xlsx')]

        if not files:
            print("[ERROR] Не найдены файлы sales_price_trend_*.xlsx")
            sys.exit(1)

        latest_file = max(files, key=lambda x: os.path.getmtime(x))

        print(f"[INFO] Конвертирую файл: {latest_file}")
        convert_xlsx_to_json(latest_file)
