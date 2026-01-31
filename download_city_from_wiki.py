import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
import time
import csv
import re

# Настройка геокодера
geolocator = Nominatim(user_agent="russian_cities_parser")

# Получаем данные с Википедии
url = "https://ru.wikipedia.org/wiki/Список_городов_России"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Находим таблицу с городами
target_table = None
for table in soup.find_all('table'):
    headers = [th.get_text(strip=True) for th in table.find_all('th')]
    if 'Город' in headers and 'Регион' in headers:
        target_table = table
        break

if not target_table:
    raise ValueError("Таблица с городами не найдена")

# Функция для очистки названия города
def clean_city_name(name):
    # Удаляем ненужные части строки, такие как "не призн."
    name = re.sub(r'\s*не призн\.?', '', name).strip()
    # Исправляем случаи типа "АчхойМартан" на "Ачхой-Мартан"
    name = re.sub(r'([а-яА-Я]+)([А-Я][а-я]+)', r'\1-\2', name)
    # Исправляем случаи типа "СлавянскнаКубани" на "Славянск-на-Кубани"
    name = re.sub(r'([а-яА-Я]+)(на)([А-Я][а-я]+)', r'\1-\2-\3', name)
    name = re.sub(r'[^\w\s-]', '', name).strip()
    return name

# Считываем существующие данные из файла и фильтруем города без координат
existing_cities = {}
try:
    with open('russian_cities.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['latitude'] and row['longitude']:  # Проверяем наличие координат
                existing_cities[row['city_name']] = row
except FileNotFoundError:
    pass

# Списки для новых городов и обновленных данных
new_cities = []
updated_cities = []

# Обрабатываем каждую строку таблицы
for row in target_table.find_all('tr')[1:]:
    cols = row.find_all('td')
    if len(cols) < 9:
        continue

    city_name = clean_city_name(cols[2].get_text(strip=True).split('[')[0].strip())
    print(f"Обработка города: {city_name}")
    region = cols[3].get_text(strip=True).split('[')[0].strip()
    federal_district = cols[4].get_text(strip=True)
    population = cols[5].get_text(strip=True).replace(' ', '')
    foundation_year = cols[6].get_text(strip=True)
    status = cols[7].get_text(strip=True)
    old_name = cols[8].get_text(strip=True).replace('"', "'")

    city_data = {
        "city_name": city_name,
        "region": region,
        "federal_district": federal_district,
        "population": population,
        "foundation_year": foundation_year,
        "status": status,
        "old_name": old_name,
    }

    if city_name in existing_cities:
        existing_cities[city_name].update({
            "region": region,
            "federal_district": federal_district,
            "population": population,
            "foundation_year": foundation_year,
            "status": status,
            "old_name": old_name
        })
        updated_cities.append(existing_cities[city_name])
    else:
        new_cities.append(city_data)

# Получаем координаты для новых городов
cities_without_coords = []
cities_with_coords = []

for city in new_cities:
    try:
        print(f"Получение координат для города: {city['city_name']}")
        location = geolocator.geocode(f"{city['city_name']}, {city['region']}, Россия", timeout=10)
        time.sleep(1)
        if location:
            city['latitude'] = location.latitude
            city['longitude'] = location.longitude
            cities_with_coords.append(city)
        else:
            cities_without_coords.append(city)
    except Exception as e:
        print(f"Ошибка геокодирования для {city['city_name']}: {str(e)}")
        cities_without_coords.append(city)

# Записываем обновленные и новые данные обратно в основной файл
with open('russian_cities.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = [
        "city_name", "region", "federal_district",
        "population", "foundation_year", "status",
        "old_name", "latitude", "longitude"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(updated_cities)
    writer.writerows(cities_with_coords)

# Записываем города без координат в отдельный файл
with open('cities_without_coords.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = [
        "city_name", "region", "federal_district",
        "population", "foundation_year", "status", "old_name"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(cities_without_coords)

print("Готово! Данные обновлены в russian_cities.csv и города без координат сохранены в cities_without_coords.csv")



