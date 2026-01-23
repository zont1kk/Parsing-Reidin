import pandas as pd
import json
import os
import sys

def convert_xlsx_to_json(xlsx_file):

    print(f"\n[START] Конвертация: {os.path.basename(xlsx_file)}")

    df = pd.read_excel(xlsx_file, skiprows=2)

    print(f"[INFO] Прочитано строк: {len(df)}")
    print(f"[INFO] Колонки: {df.columns.tolist()}")

    if 'Rental Yields (%)' not in df.columns:
        print(f"[ERROR] Не найдена колонка 'Rental Yields (%)'")
        return None

    # Очищаем данные
    df = df.dropna(subset=['Date', 'Rental Yields (%)', 'Location'])

    print(f"[INFO] После очистки: {len(df)} строк")

    if len(df) == 0:
        print(f"[WARNING] Нет данных для конвертации")

        json_file = xlsx_file.replace('.xlsx', '.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump({}, f, ensure_ascii=False, indent=2)
        print(f"[OK] Создан пустой JSON: {os.path.basename(json_file)}")

        # try:
        #     os.remove(xlsx_file)
        #     print(f"[OK] Удален исходный файл: {os.path.basename(xlsx_file)}")
        # except Exception as e:
        #     print(f"[WARNING] Не удалось удалить файл {xlsx_file}: {e}")

        return json_file

    # Форматируем дату
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%d.%m.%Y')

    # Структура: {date: {location: rental_yields_percent}}
    result = {}

    for _, row in df.iterrows():
        date = row['Date']
        location = row['Location']
        rental_yields = round(float(row['Rental Yields (%)']), 2)

        if date not in result:
            result[date] = {}

        result[date][location] = {
            'rental_yields_percent': rental_yields
        }

    json_file = xlsx_file.replace('.xlsx', '.json')

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    total_records = sum(len(locations) for locations in result.values())

    print(f"[OK] Конвертация завершена")
    print(f"[INFO] Уникальных дат: {len(result)}")
    print(f"[INFO] Всего записей: {total_records}")
    print(f"[INFO] Сохранено в: {os.path.basename(json_file)}")

    # try:
    #     os.remove(xlsx_file)
    #     print(f"[OK] Удален исходный файл: {os.path.basename(xlsx_file)}")
    # except Exception as e:
    #     print(f"[WARNING] Не удалось удалить файл {xlsx_file}: {e}")

    return json_file

if __name__ == "__main__":

    xlsx_file = os.environ.get('XLSX_FILE') or (sys.argv[1] if len(sys.argv) > 1 else None)

    if not xlsx_file:
        print("[ERROR] Не указан файл для конвертации")
        sys.exit(1)

    if not os.path.exists(xlsx_file):
        print(f"[ERROR] Файл не найден: {xlsx_file}")
        sys.exit(1)

    result_file = convert_xlsx_to_json(xlsx_file)

    if result_file:
        print(f"\n[SUCCESS] Конвертация успешна: {os.path.basename(result_file)}")
    else:
        print("\n[ERROR] Конвертация не удалась")
        sys.exit(1)
