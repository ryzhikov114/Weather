import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

try:
    connection = psycopg2.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        dbname=os.getenv("dbname")
    )
    
    cursor = connection.cursor()
    
    #cursor.execute("DROP TABLE IF EXISTS lbn.weather_BUFFER")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lbn.weather_BUFFER (
            date TIMESTAMP,
            temperature_2m FLOAT,
            wind_speed_10m FLOAT,
            wind_direction_10m FLOAT,
            apparent_temperature FLOAT,
            precipitation FLOAT,
            rain FLOAT,
            showers FLOAT,
            snowfall FLOAT,
            snow_depth FLOAT,
            is_day FLOAT,
            sunshine_duration FLOAT,
            latitude FLOAT,
            longitude FLOAT
        )
    """)

    #cursor.execute("DROP TABLE IF EXISTS lbn.weather")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lbn.weather (
            date TIMESTAMP,
            temperature_2m REAL,
            wind_speed_10m REAL,
            wind_direction_10m REAL,
            apparent_temperature REAL,
            precipitation REAL,
            rain REAL,
            showers REAL,
            snowfall REAL,
            snow_depth REAL,
            is_day BOOLEAN,
            sunshine_duration INT,
            latitude FLOAT,
            longitude FLOAT,
            DATE_UPDATE TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (latitude, longitude, date)
        )
    """)    

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_weather_date ON lbn.weather (date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_weather_long_lat ON lbn.weather (latitude,longitude)")


    cursor.execute("""DROP TABLE lbn.city_BUFFER""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lbn.city_BUFFER (
            city_name VARCHAR(4000),
            region VARCHAR(4000),
            federal_district VARCHAR(4000),
            population VARCHAR(4000),
            foundation_year VARCHAR(4000),
            status VARCHAR(4000),
            old_name VARCHAR(4000),
            latitude VARCHAR(4000),
            longitude VARCHAR(4000)
        );
        """)

    cursor.execute("""DROP TABLE lbn.city""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lbn.city (
            city_name VARCHAR(500) NOT NULL,
            region VARCHAR(500),
            federal_district VARCHAR(500),
            population INT,
            foundation_year VARCHAR(500),
            status VARCHAR(500),
            old_name VARCHAR(500),
            latitude FLOAT,
            longitude FLOAT,
            date_update TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (latitude, longitude)
        );
        """)

    connection.commit()
    cursor.close()
    connection.close()
    print('Скрипт выполнен')

except Exception as e:
    print(f"Error: {e}")