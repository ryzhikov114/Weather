import requests
import json
import psycopg2
from datetime import datetime
import time
import logging
import argparse
from dotenv import load_dotenv
import os

# Настройка логов
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('dtp_loader.log')
    ]
)
logger = logging.getLogger()

# Загрузка настроек
load_dotenv()

# Параметры подключения
DB_PARAMS = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port")
}

# Список городов
CITIES = [
    {"name": "Лобня", "region_id": "46", "district_id": "46440"},
    {"name": "Калининград", "region_id": "27", "district_id": "27401"}
]

def parse_args():
    parser = argparse.ArgumentParser(description="Загрузка данных о ДТП за указанный период.")
    parser.add_argument("--start_year", type=int, help="Начальный год")
    parser.add_argument("--start_month", type=int, help="Начальный месяц")
    parser.add_argument("--end_year", type=int, help="Конечный год")
    parser.add_argument("--end_month", type=int, help="Конечный месяц")
    return parser.parse_args()

def get_date_range(args):
    now = datetime.now()
    default_end_year = now.year
    default_end_month = now.month

    default_start_month = default_end_month - 5
    default_start_year = default_end_year

    if default_start_month <= 0:
        default_start_month += 12
        default_start_year -= 1

    start_year = args.start_year if args.start_year is not None else default_start_year
    start_month = args.start_month if args.start_month is not None else default_start_month
    end_year = args.end_year if args.end_year is not None else default_end_year
    end_month = args.end_month if args.end_month is not None else default_end_month

    return start_year, start_month, end_year, end_month

def fetch_data_from_api(payload):
    logger.info("Отправляем запрос к API...")
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
    response = requests.post(
        "http://stat.gibdd.ru/map/getDTPCardData",
        json=payload,
        headers=headers,
        timeout=45
    )
    response.raise_for_status()
    logger.info("Запрос к API выполнен успешно.")
    return response.json()

def insert_single_record(record, index):
    max_attempts = 5
    for attempt in range(max_attempts):
        conn = None
        try:
            logger.debug(f"Попытка {attempt + 1} вставки записи {index + 1}")

            # Подключаемся с автокоммитом
            conn = psycopg2.connect(**DB_PARAMS)
            conn.autocommit = True  # Включаем автокоммит
            
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO lbn.dtp_BUFFER
                    (city_name, region_id, district_id, raw_json)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        record["city_name"],
                        record["region_id"],
                        record["district_id"],
                        record["raw_json"]
                    )
                )
            
            logger.debug(f"Запись {index + 1} успешно вставлена.")
            return True
            
        except psycopg2.OperationalError as e:
            logger.error(f"Ошибка вставки записи {index + 1} (попытка {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Неожиданная ошибка при вставке записи {index + 1}: {e}")
            return False
        finally:
            # Всегда закрываем соединение
            if conn and not conn.closed:
                conn.close()
    
    logger.error(f"Не удалось вставить запись {index + 1} после {max_attempts} попыток.")
    return False

def main():
    args = parse_args()
    start_year, start_month, end_year, end_month = get_date_range(args)

    logger.info(f"Загрузка данных с {start_month}.{start_year} по {end_month}.{end_year}")

    # Список для хранения всех данных
    all_records = []
    failed_records = []

    for city in CITIES:
        logger.info(f"Обработка города: {city['name']}")

        for year in range(start_year, end_year + 1):
            start_m = start_month if year == start_year else 1
            end_m = end_month if year == end_year else 12

            for month in range(start_m, end_m + 1):
                logger.info(f"Загрузка данных за {month}.{year}...")

                payload = {
                    "data": json.dumps({
                        "date": [f"MONTHS:{month}.{year}"],
                        "ParReg": city["region_id"],
                        "order": {"type": "1", "fieldName": "dat"},
                        "reg": city["district_id"],
                        "ind": "1",
                        "st": "1",
                        "en": "1000",
                        "fil": {"isSummary": False},
                        "fieldNames": ["dat", "time", "coordinates", "infoDtp"]
                    }, separators=(',', ':'))
                }

                try:
                    response_json = fetch_data_from_api(payload)
                    logger.info("Данные от API получены.")

                    if "data" not in response_json:
                        logger.warning("Нет поля 'data' в ответе API")
                        continue

                    try:
                        data = json.loads(response_json["data"]).get("tab", [])
                    except json.JSONDecodeError as e:
                        logger.warning(f"Невалидный JSON в ответе: {e}")
                        continue

                    if not data:
                        logger.info("Нет данных для загрузки")
                        continue

                    # Сохраняем данные во временный список
                    for record in data:
                        all_records.append({
                            "city_name": city["name"],
                            "region_id": city["region_id"],
                            "district_id": city["district_id"],
                            "raw_json": json.dumps(record, ensure_ascii=False)
                        })

                    logger.info(f"Получено {len(data)} записей за {month}.{year}")

                except requests.exceptions.RequestException as e:
                    logger.error(f"Ошибка запроса: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Неожиданная ошибка: {e}", exc_info=True)
                    continue

                time.sleep(1)  # Пауза между месяцами

    # Вставляем данные в базу по одной записи
    if all_records:
        logger.info(f"Начинаем вставку {len(all_records)} записей в базу данных...")
        success_count = 0
        for index, record in enumerate(all_records):
            logger.info(f"Вставляем запись {index + 1} из {len(all_records)}...")
            if insert_single_record(record, index):
                success_count += 1
            else:
                failed_records.append(record)
                logger.warning(f"Не удалось вставить запись {index + 1}.")

        logger.info(f"Успешно вставлено {success_count} записей из {len(all_records)}.")

        # Сохраняем невставленные записи в файл
        if failed_records:
            logger.warning(f"Не удалось вставить {len(failed_records)} записей. Сохраняем их в failed_records.json...")
            with open('failed_records.json', 'w', encoding='utf-8') as f:
                json.dump(failed_records, f, ensure_ascii=False)
    else:
        logger.info("Нет данных для вставки в базу.")

    logger.info("Работа скрипта завершена")

if __name__ == "__main__":
    main()
