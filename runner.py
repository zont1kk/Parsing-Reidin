"""
Главный скрипт для запуска парсера на батчах районов
Запускает parser.py для каждого файла районов, объединяет результаты и делает трансформацию
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

def load_config() -> Dict[str, Any]:
    """Загружает конфиг"""
    if not os.path.exists("config.json"):
        print("[ERROR] Файл config.json не найден")
        sys.exit(1)
    with open("config.json", "r", encoding="utf-8") as f:
        config: Dict[str, Any] = json.load(f)
        return config

def load_areas_and_create_batches(config: Dict[str, Any]) -> Optional[List[List[str]]]:
    """Загружает районы из одного файла и разбивает на батчи"""
    areas_file = config["areas_file"]
    batch_size = config["batch_size"]
    
    if not os.path.exists(areas_file):
        print(f"[ERROR] Файл {areas_file} не найден")
        return None
    
    with open(areas_file, "r", encoding="utf-8") as f:
        areas = [line.strip() for line in f if line.strip()]
    
    if not areas:
        print(f"[ERROR] Файл {areas_file} пуст")
        return None
    
    print(f"[OK] Загружено {len(areas)} районов из {areas_file}")
    
    batches = []
    for i in range(0, len(areas), batch_size):
        batch = areas[i:i+batch_size]
        batches.append(batch)
    
    print(f"[OK] Разбито на {len(batches)} батчей по {batch_size} районов")
    return batches if batches else None

def run_parser_for_batch(areas_batch: List[str], output_raw_file: str, batch_num: int, total_batches: int, date_settings: Dict[str, Any]) -> bool:
    """Запускает parser.py для батча районов"""
    batch_areas_count = len(areas_batch)
    print(f"\n{'='*70}")
    print(f"[BATCH {batch_num}/{total_batches}] Обработка {batch_areas_count} районов")
    print(f"[OUTPUT] Raw file: {output_raw_file}")
    print(f"[DATES] {date_settings['start_date']} - {date_settings['end_date']} (everyday: {date_settings['everyday']})")
    print(f"{'='*70}")
    
    with open("areas.txt", "w", encoding="utf-8") as f:
        for area in areas_batch:
            f.write(area + "\n")
    
    try:
        venv_python = os.path.join(os.getcwd(), "venv", "Scripts", "python.exe")
        if not os.path.exists(venv_python):
            venv_python = "python"
        env = os.environ.copy()
        env["PARSER_START_DATE"] = date_settings["start_date"]
        env["PARSER_END_DATE"] = date_settings["end_date"]
        env["PARSER_EVERYDAY"] = str(date_settings["everyday"]).lower()
        env["PARSER_OUTPUT_RAW_FILE"] = output_raw_file
        result = subprocess.run([venv_python, "parser.py"], env=env)
        if result.returncode != 0:
            print(f"[ERROR] Парсер завершился с ошибкой (код {result.returncode})")
            return False
        print(f"\n[OK] Батч {batch_num} завершен успешно")
        return True
    except Exception as e:
        print(f"[ERROR] Ошибка при запуске парсера: {e}")
        return False

def merge_raw_files(total_batches: int, output_raw_file: str, output_merged_file: str) -> bool:
    """Объединяет все батчи в один файл"""
    print(f"\n{'='*70}")
    print(f"[MERGE] Объединяю результаты...")
    print(f"[INPUT] {output_raw_file} (один для каждого батча)")
    print(f"[OUTPUT] {output_merged_file}")
    print(f"{'='*70}")
    merged_data: Dict[str, Dict[str, Any]] = {}
    raw_file = output_raw_file
    if not os.path.exists(raw_file):
        print(f"[WARNING] Файл {raw_file} не найден")
        return False
    print(f"[INFO] Загружаю {raw_file}...")
    with open(raw_file, "r", encoding="utf-8") as f:
        batch_data = json.load(f)
    for date_str, areas_dict in batch_data.items():
        if date_str not in merged_data:
            merged_data[date_str] = {}
        merged_data[date_str].update(areas_dict)
        print(f"  [{date_str}] Загружено {len(areas_dict)} районов")
    with open(output_merged_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)
    total_dates = len(merged_data)
    total_areas = sum(len(date_data) for date_data in merged_data.values())
    print(f"\n[OK] Объединение завершено!")
    print(f"  Дат: {total_dates}")
    print(f"  Всего уникальных районов: {len(set(area for date_data in merged_data.values() for area in date_data.keys()))}")
    print(f"  Файл: {output_merged_file}")
    return True

def run_transform(output_final_file: str) -> bool:
    """Запускает скрипт трансформации"""
    print(f"\n{'='*70}")
    print(f"[TRANSFORM] Запуск трансформации...")
    print(f"[OUTPUT] {output_final_file}")
    print(f"{'='*70}")
    try:
        venv_python = os.path.join(os.getcwd(), "venv", "Scripts", "python.exe")
        if not os.path.exists(venv_python):
            venv_python = "python"
        
        env = os.environ.copy()
        env["TRANSFORM_OUTPUT_FILE"] = output_final_file
        
        result = subprocess.run([venv_python, "transform_to_structure.py"], env=env)
        if result.returncode != 0:
            print(f"[ERROR] Трансформация завершилась с ошибкой (код {result.returncode})")
            return False
        print(f"\n[OK] Трансформация завершена успешно")
        return True
    except Exception as e:
        print(f"[ERROR] Ошибка при запуске трансформации: {e}")
        return False

def main() -> None:
    print(f"{'='*70}")
    print(f"[START] Главный скрипт запуска парсера Reidin")
    print(f"[TIME] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    print(f"\n[INFO] Загружаю config.json...")
    config = load_config()
    
    auto_mode = config["auto"]
    
    if auto_mode:
        print(f"[INFO] Режим: AUTO")
        today = datetime.now()
        days_since_monday = today.weekday()
        last_sunday = today - timedelta(days=days_since_monday + 1)
        last_monday = last_sunday - timedelta(days=6)
        
        start_date_str = last_monday.strftime("%d.%m.%Y")
        end_date_str = last_sunday.strftime("%d.%m.%Y")
        
        date_settings = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "everyday": True
        }
        print(f"[INFO] Автоматический режим: прошлая неделя ({start_date_str} - {end_date_str})")
        week_range = f"{start_date_str.replace('.', '_')}-{end_date_str.replace('.', '_')}"
        output_final_file = f"week_{week_range}.json"
    else:
        print(f"[INFO] Режим: MANUAL")
        date_settings = config["date_settings"]
        output_final_file = config["output_final_file"]
    
    output_raw_file = config["output_raw_file"]
    output_merged_file = config["output_merged_file"]
    
    print(f"\n[INFO] Параметры дат:")
    print(f"  Start: {date_settings['start_date']}")
    print(f"  End: {date_settings['end_date']}")
    print(f"  Everyday: {date_settings['everyday']}")
    
    print(f"\n[INFO] Загружаю районы и создаю батчи:")
    batches = load_areas_and_create_batches(config)
    if not batches:
        sys.exit(1)
    
    total_batches = len(batches)
    
    output_raw_path = os.path.join(os.getcwd(), output_raw_file)
    if os.path.exists(output_raw_path):
        os.remove(output_raw_path)
        print(f"[INFO] Удалил старый файл: {output_raw_file}")
    
    for batch_num, batch_areas in enumerate(batches, 1):
        run_parser_for_batch(batch_areas, output_raw_file, batch_num, total_batches, date_settings)
    
    merge_raw_files(total_batches, output_raw_file, output_merged_file)
    
    if os.path.exists(output_merged_file):
        with open(output_merged_file, "r", encoding="utf-8") as f:
            merged = json.load(f)
        with open(output_raw_file, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        print(f"[OK] Скопировал {output_merged_file} в {output_raw_file}")
    
    transform_success = run_transform(output_final_file)
    
    print(f"\n{'='*70}")
    print(f"[FINISH] Завершено!")
    print(f"[TIME] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    if os.path.exists(output_final_file):
        with open(output_final_file, "r", encoding="utf-8") as f:
            final_data = json.load(f)
        total_dates = len(final_data)
        total_areas = len(set(area for date_data in final_data.values() for area in date_data.keys()))
        print(f"\nФинальный результат: {output_final_file}")
        print(f"  Дат: {total_dates}")
        print(f"  Районов: {total_areas}")
    
    print(f"\n[FILES]")
    print(f"  Raw: {output_merged_file}")
    print(f"  Final: {output_final_file}")

if __name__ == "__main__":
    main()
