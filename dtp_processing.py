import json
import psycopg2
from psycopg2 import pool
from datetime import datetime
from dotenv import load_dotenv
import os
import logging
import sys
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dtp_processing.log', mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Загрузка настроек
load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port")
}

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%d.%m.%Y').date() if date_str else None
    except:
        return None

def parse_time(time_str):
    try:
        return datetime.strptime(time_str, '%H:%M').time() if time_str else None
    except:
        return None

def parse_int(value):
    try:
        return int(value) if value is not None else 0
    except:
        return 0

def parse_float(value):
    try:
        return float(str(value).replace(',', '.')) if value is not None else 0.0
    except:
        return 0.0

def safe_join(value):
    """Безопасное объединение элементов в строку"""
    if isinstance(value, list):
        try:
            return ', '.join(str(item) for item in value if item is not None)
        except:
            return str(value)
    return str(value) if value is not None else ''

def get_connection():
    """Получаем соединение с повторными попытками"""
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            return conn
        except Exception as e:
            logger.warning(f"Попытка {attempt + 1}: Ошибка подключения: {e}")
            if attempt < max_attempts - 1:
                time.sleep(2 ** attempt)
    return None

def main():
    logger.info("=" * 60)
    logger.info("НАЧАЛО ОБРАБОТКИ ДТП")
    logger.info(f"Время запуска: {datetime.now()}")
    
    # Получаем соединение
    conn = get_connection()
    if not conn:
        logger.error("Не удалось подключиться к базе данных")
        return
    
    try:
        with conn.cursor() as cur:
            # Простая проверка - считаем записи в dtp_buffer
            try:
                cur.execute("SELECT COUNT(*) FROM lbn.dtp_buffer")
                total_records = cur.fetchone()[0]
                logger.info(f"Всего записей в dtp_buffer: {total_records}")
                
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM lbn.dtp_buffer 
                    WHERE date_processing IS NULL AND is_error = FALSE
                """)
                to_process = cur.fetchone()[0]
                logger.info(f"Записей для обработки: {to_process}")
                
                if to_process == 0:
                    logger.info("Нет записей для обработки. Завершаем работу.")
                    return
                    
            except Exception as e:
                logger.error(f"Ошибка при проверке таблиц: {e}")
                logger.info("Продолжаем работу без детальной проверки таблиц")
        
        # Начинаем обработку
        processed_count = 0
        error_count = 0
        batch_size = 10  # Малый размер для начала
        
        logger.info(f"Начинаем обработку {to_process} записей...")
        
        while True:
            # Получаем новое соединение для каждого цикла
            batch_conn = get_connection()
            if not batch_conn:
                logger.error("Не удалось получить соединение для батча")
                break
                
            try:
                with batch_conn.cursor() as cur:
                    # Получаем записи для обработки
                    cur.execute("""
                        SELECT id, region_id, district_id, raw_json, city_name
                        FROM lbn.dtp_buffer
                        WHERE date_processing IS NULL
                        AND is_error = FALSE
                        ORDER BY id
                        LIMIT %s
                    """, (batch_size,))
                    
                    rows = cur.fetchall()
                    if not rows:
                        logger.info("Больше нет записей для обработки")
                        break
                    
                    logger.info(f"Найдено {len(rows)} записей для обработки")
                    
                    for i, row in enumerate(rows):
                        id, region_id, district_id, raw_json, city_name = row
                        city_name = city_name if city_name else "Не указан"
                        
                        logger.info(f"[{i+1}/{len(rows)}] Обработка id={id}")
                        
                        # Обрабатываем каждую запись в отдельном соединении
                        record_conn = get_connection()
                        if not record_conn:
                            logger.error(f"Не удалось получить соединение для записи id={id}")
                            continue
                            
                        try:
                            with record_conn.cursor() as record_cur:
                                # Парсинг JSON
                                try:
                                    if isinstance(raw_json, dict):
                                        data_list = [raw_json]
                                    else:
                                        data = json.loads(raw_json)
                                        if isinstance(data, dict):
                                            data_list = [data]
                                        elif isinstance(data, list):
                                            data_list = data
                                        else:
                                            raise ValueError("Некорректный формат JSON")
                                except Exception as e:
                                    logger.error(f"ID {id}: Ошибка парсинга JSON: {e}")
                                    raise
                                
                                for data in data_list:
                                    if not isinstance(data, dict):
                                        logger.warning(f"ID {id}: Пропуск - не dict")
                                        continue
                                    
                                    kart_id = data.get('KartId')
                                    if not kart_id:
                                        logger.warning(f"ID {id}: Пропуск - нет KartId")
                                        continue
                                    
                                    info = data.get('infoDtp', {})
                                    
                                    # Обработка dtp_main
                                    dtp_date = parse_date(data.get('date'))
                                    dtp_time = parse_time(data.get('Time'))
                                    settlement = info.get('n_p', city_name)
                                    
                                    # Вставляем основную информацию (без удаления старых записей пока)
                                    try:
                                        record_cur.execute("""
                                            INSERT INTO lbn.dtp_main (
                                                kart_id, region_id, district_id, row_num, dtp_date, dtp_time, district,
                                                dtp_type, deaths, wounded, vehicles_count, participants_count, emtp_number,
                                                settlement, street, house, road, km, m, road_category, road_class,
                                                road_quality, weather, road_condition, lighting, dtp_severity, coord_w, coord_l
                                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                      %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        """, (
                                            kart_id, region_id, district_id, parse_int(data.get('rowNum')), dtp_date, dtp_time,
                                            data.get('District', ''), data.get('DTP_V', ''), parse_int(data.get('POG', 0)),
                                            parse_int(data.get('RAN', 0)), parse_int(data.get('K_TS', 0)),
                                            parse_int(data.get('K_UCH', 0)), data.get('emtp_number', ''), settlement,
                                            info.get('street', ''), info.get('house', ''), info.get('dor', ''),
                                            info.get('km', ''), info.get('m', ''), info.get('k_ul', ''),
                                            info.get('dor_z', ''), info.get('s_pch', ''), info.get('osv', ''),
                                            info.get('sdor', ''), info.get('change_org_motion', ''),
                                            info.get('s_dtp', ''), parse_float(info.get('COORD_W', 0.0)),
                                            parse_float(info.get('COORD_L', 0.0))
                                        ))
                                        logger.debug(f"ID {id}: dtp_main вставлен")
                                        
                                    except psycopg2.errors.UniqueViolation:
                                        # Если запись уже существует - обновляем
                                        logger.debug(f"ID {id}: Запись уже существует, обновляем")
                                        # Можно добавить UPDATE здесь
                                        pass
                                    except Exception as e:
                                        logger.error(f"ID {id}: Ошибка вставки dtp_main: {e}")
                                        raise
                                    
                                    # Помечаем как обработанную
                                    record_cur.execute("""
                                        UPDATE lbn.dtp_buffer
                                        SET date_processing = CURRENT_TIMESTAMP
                                        WHERE id = %s
                                    """, (id,))
                                    
                                    processed_count += 1
                                    logger.info(f"ID {id}: Успешно обработано (всего: {processed_count})")
                                    
                        except Exception as e:
                            error_count += 1
                            logger.error(f"✗ ID {id}: Ошибка: {e}")
                            
                            # Пробуем пометить как ошибку
                            try:
                                error_conn = get_connection()
                                if error_conn:
                                    with error_conn.cursor() as error_cur:
                                        error_cur.execute("""
                                            UPDATE lbn.dtp_buffer
                                            SET is_error = TRUE
                                            WHERE id = %s
                                        """, (id,))
                                    error_conn.close()
                            except:
                                pass
                                
                        finally:
                            if record_conn:
                                record_conn.close()
                        
                        # Пауза между записями
                        time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Ошибка в цикле обработки: {e}")
            finally:
                if batch_conn:
                    batch_conn.close()
            
            # Пауза между батчами
            logger.info(f"Батч обработан. Пауза 2 секунды...")
            time.sleep(2)
        
        logger.info("=" * 60)
        logger.info(f"ОБРАБОТКА ЗАВЕРШЕНА:")
        logger.info(f"  Всего обработано: {processed_count}")
        logger.info(f"  Ошибок: {error_count}")
        logger.info(f"  Время завершения: {datetime.now()}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Соединение закрыто")

if __name__ == "__main__":
    main()