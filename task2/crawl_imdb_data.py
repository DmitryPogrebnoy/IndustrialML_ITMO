import os
from typing import List, Tuple

import pandas as pd
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NUMBER_OF_FILMS_PER_CATEGORIES = 300
NUMBER_FILMS_ON_PAGE = 50
CATEGORIES = ["action", "adventure", "animation", "biography", "comedy", "crime", "documentary", "drama", "family",
              "fantasy", "film_noir", "history", "horror", "music", "musical", "mystery", "romance", "sci_fi",
              "short", "sport", "superhero", "thriller", "war", "western"]

PATH_TO_SAVE_POSTERS = "./data/posters/"
PATH_TO_SAVE_GLOBAL_DATAFRAME = "./data/crawled_data.csv"
PATH_TO_SAVE_LOCAL_DATAFRAME = "./data/"

FILTER_HREFS_REGEXP = re.compile("^\/title\/tt.*")
EXTRACT_ID_REGEXP = re.compile("tt\d*")

IMDB_GENRE_URL = 'https://www.imdb.com/search/title/?genres='
IMDB_TITLE_URL = 'https://www.imdb.com/title/'

session = requests.Session()
retry = Retry(connect=5, backoff_factor=1)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)


def get_html(url):
    response = session.get(url, headers={'Accept-Language': 'en'})
    print(response)
    return BeautifulSoup(response.content, 'html.parser')


def crawl_genre_ids(genre: str) -> List[str]:
    def get_links_to_film_pages(html) -> List[str]:
        divs = html.findAll("div", {"class": "lister-item-image"})
        a_nested_tags = [div.findAll("a", href=True) for div in divs]
        a_tags = [a_tag for nested_list in a_nested_tags for a_tag in nested_list]
        hrefs = [tag["href"] for tag in a_tags]
        valid_hrefs = [href for href in hrefs if FILTER_HREFS_REGEXP.match(href)]
        title_ids = [EXTRACT_ID_REGEXP.search(id).group(0) for id in valid_hrefs]
        return title_ids

    unique_ids = []
    for i in range(1, NUMBER_OF_FILMS_PER_CATEGORIES, NUMBER_FILMS_ON_PAGE):
        url = IMDB_GENRE_URL + genre.lower() + "&start=" + str(i)
        print(f"Crawl ids from: {url}")
        html = get_html(url)
        unique_ids.extend(get_links_to_film_pages(html))
    return unique_ids


def crawl_film_page(url: str) -> Tuple[str, str, str, str]:
    html = get_html(url)
    while str(html.find("h1").contents[0]) == "Error":
        print(f"Error page received! Try to request again! Link {url}")
        html = get_html(url)
    title = str(html.find("h1", {"class": "sc-b73cd867-0"}).contents[0])
    poster_link = str(html.find("img", {"class": "ipc-image"})["src"]) if html.find("img",
                                                                                    {"class": "ipc-image"}) else ""
    description_element = html.find("span", {"role": "presentation", "class": "sc-16ede01-2"})
    description = str(description_element.contents[0].string) if description_element and len(
        description_element.contents) else ""
    labels = " ".join([item.contents[0] for item in html.findAll("span", {"class": "ipc-chip__text"})])
    return title, poster_link, description, labels


def crawl_genre(genre: str) -> Tuple[List[str], List[str], List[str], List[str]]:
    title_list = []
    poster_link_list = []
    description_list = []
    labels_list = []

    uniquie_ids = crawl_genre_ids(genre)
    print(f"Collected number films id: {len(uniquie_ids)}")
    for id in tqdm(uniquie_ids):
        url = IMDB_TITLE_URL + id
        print(f"Crawl title page: {url}")
        title, poster_link, description, labels = crawl_film_page(url)

        title_list.append(title)
        poster_link_list.append(poster_link)
        description_list.append(description)
        labels_list.append(labels)

        if poster_link != "":
            poster_response = session.get(poster_link)
            if poster_response.status_code == 200:
                path_to_save = PATH_TO_SAVE_POSTERS + genre + "/" + re.sub("\\W", "_", title.casefold()) + ".jpg"
                os.makedirs(os.path.dirname(path_to_save), exist_ok=True)
                with open(path_to_save, 'w+b') as f:
                    f.write(poster_response.content)
            else:
                print(f"Poster loading error!!! Response: {poster_response}. Urs: {url}")

    return title_list, poster_link_list, description_list, labels_list


def crawl():
    genre_list = []
    title_list = []
    poster_link_list = []
    description_list = []
    labels_list = []

    for genre in CATEGORIES:
        print(f"Process genre: {genre}")
        genre_title_list, genre_poster_link_list, genre_description_list, genre_labels_list = crawl_genre(genre)
        title_list.extend(genre_title_list)
        poster_link_list.extend(genre_poster_link_list)
        description_list.extend(genre_description_list)
        labels_list.extend(genre_labels_list)
        genre_list.extend([genre] * len(genre_labels_list))

        genre_df = pd.DataFrame({"genre": [genre] * len(genre_labels_list),
                           "title": genre_title_list,
                           "poster_link": genre_poster_link_list,
                           "description": genre_description_list,
                           "labels": genre_labels_list})

        genre_df.to_csv(PATH_TO_SAVE_LOCAL_DATAFRAME + genre + "_crawled_data.csv", index=False)

    df = pd.DataFrame({"genre": genre_list,
                       "title": title_list,
                       "poster_link": poster_link_list,
                       "description": description_list,
                       "labels": labels_list})

    df.to_csv(PATH_TO_SAVE_GLOBAL_DATAFRAME, index=False)


if __name__ == "__main__":
    """
    Request and store data about films from IMDB
    """
    crawl()
