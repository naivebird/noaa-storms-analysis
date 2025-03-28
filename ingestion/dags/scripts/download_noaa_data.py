import logging

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("DownloadNoaaData")

NOAA_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"


def get_noaa_url(year, file_type):
    response = requests.get(NOAA_URL)
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table")
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if cells and cells[0].text.startswith(f"StormEvents_{file_type}-ftp_v1.0_d{year}"):
            return f"{NOAA_URL}{cells[0].find('a')['href']}"
    raise Exception(f"Failed to find the URL for the given year: {year}")


def download_noaa_data(year, file_type, output_path):
    url = get_noaa_url(year, file_type)
    logger.debug(f"Downloading {file_type} data for year {year} from {url}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, "wb") as file:
            file.write(response.content)
    else:
        raise Exception("Failed to download NOAA dataset.")
