import asyncio
import concurrent.futures
from datetime import datetime
from math import ceil
import aiohttp
import requests
import petl as etl
import uuid

from PythonChallangeTask.settings import BASE_DIR
from .models import ImportedFiles

START_URL = "https://swapi.dev/api/people/?page=1"
PART_URL =  "https://swapi.dev/api/people/?page="

def generate_unique_filename(extension):
    return str(uuid.uuid4()) + extension


def prepare_urls(url_source, url_part):
    urls = [url_source]
    response = requests.get(url_source)
    data = response.json()
    page_numbers = ceil(data["count"] / len(data["results"])) + 1

    for i in range(2, page_numbers):
        urls.append(url_part + str(i))
    return urls


def get_data_from_api_async(urls):
    result_pages = []
    result_rows = []

    async def fetch_url(session, url):
        async with session.get(url) as response:
            return await response.json()

    async def fetch_all_urls(urls):
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_url(session, url) for url in urls]
            return await asyncio.gather(*tasks)

    def run_async(coro):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)

    def main():
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [executor.submit(run_async, fetch_all_urls(urls[i:i + 2])) for i in range(0, len(urls), 1)]
            for future in concurrent.futures.as_completed(results):
                res = future.result()[0]
                result_pages.append(res.items())
        for pages in result_pages:
            for result in pages:
                result_rows.append(result)

        return result_rows

    return main()


def get_and_map_data():
    result = []
    people_urls = prepare_urls(START_URL,PART_URL)
    people_result = get_data_from_api_async(people_urls)
    planet_urls = set()
    for key, value in people_result:
        if key == 'results':
            for row in value:
                result_row = {
                    'name': row['name'],
                    'height': row['height'],
                    'mass': row['mass'],
                    'hair_color': row['hair_color'],
                    'skin_color': row['skin_color'],
                    'eye_color': row['eye_color'],
                    'birth_year': row['birth_year'],
                    'gender': row['gender'],
                    'homeworld': row['homeworld'],
                    'date': datetime.strptime(row['edited'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")}
                result.append(result_row)
                planet_urls.add(row['homeworld'])

    planets = get_data_from_api_async(list(planet_urls))
    for key, value in planets:
        if key == 'name':
            name = value
        if key == 'url':
            for element in result:
                if element['homeworld'] == value:
                    element['homeworld'] = name
    return result


def save_data_to_csv(data):
    file_name = generate_unique_filename('.csv')
    table = etl.fromdicts(data)
    etl.tocsv(table, f'{BASE_DIR}/files/{file_name}', encoding='utf-8')
    return file_name


def save_metadata(file_name):
    imported_file = ImportedFiles()
    imported_file.file_name = file_name
    imported_file.save()


def read_from_csv(file_name, row_numbers):
    table = etl.fromcsv(f'{BASE_DIR}/files/{file_name}')
    rows = etl.head(table, n=row_numbers)
    return rows.dicts()


def read_aggregated_data(file_name, columns):
    table = etl.fromcsv(f'{BASE_DIR}/files/{file_name}')
    if len(columns) == 0:
        return {}
    aggregated_table = etl.aggregate(table, columns[0] if len(columns) == 1 else columns, len)
    result = etl.sort(aggregated_table, 'value', reverse=True)
    return result.dicts()


def read_csv_header(file_name):
    table = etl.fromcsv(f'{BASE_DIR}/files/{file_name}')
    columns = etl.header(table)

    return columns
