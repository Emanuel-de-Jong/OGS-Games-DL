import os
import requests
import json
import time
import random
import re
from dateutil import parser

MIN_MOVES = 50
INCLUDE_CANCELLATIONS = False
INCLUDE_NON_19X19 = False
INCLUDE_REVIEWS = False
USERNAME = "Emanuel de Jong"
OLD_USERNAMES = ["Grav1ton", "gamersfunnl", "KillBottt"]

DATA_PATH = "data"
CACHE_PATH = "cache"
RESULTS_PATH = "results"
CACHE_FILE_GAMES = "games_response_data.json"
CACHE_FILE_DEMOS = "demos_response_data.json"
PROCESSED_IDS_FILE = "processed_ids.json"
RESPONSE_DATA_GAMES_PATH = os.path.join(CACHE_PATH, CACHE_FILE_GAMES)
RESPONSE_DATA_DEMOS_PATH = os.path.join(CACHE_PATH, CACHE_FILE_DEMOS)
PROCESSED_IDS_PATH = os.path.join(CACHE_PATH, PROCESSED_IDS_FILE)
RESULTS_PATH_GAMES = os.path.join(RESULTS_PATH, "games")
RESULTS_PATH_DEMOS = os.path.join(RESULTS_PATH, "demos")
RATE_LIMIT_DELAY = 62
MAX_REQUEST_TRIES = 2


def main():
    ensure_directories_exists()

    request_meta = load_postman_collection()
    response_data_games, response_data_demos = get_data(request_meta)
    if not response_data_games or not response_data_demos:
        return
    
    save_demo_urls(response_data_demos)
    # return

    processed_ids = load_processed_ids()

    if response_data_games:
        result = filter_and_download_matches(response_data_games, RESULTS_PATH_GAMES, request_meta, processed_ids)
        if not result:
            return

    if response_data_demos:
        result = filter_and_download_matches(response_data_demos, RESULTS_PATH_DEMOS, request_meta, processed_ids, True)
        if not result:
            return


def ensure_directories_exists():
    os.makedirs(CACHE_PATH, exist_ok=True)
    os.makedirs(RESULTS_PATH_GAMES, exist_ok=True)
    os.makedirs(RESULTS_PATH_DEMOS, exist_ok=True)


def load_postman_collection():
    postman_json = None
    for file_name in os.listdir(DATA_PATH):
        if file_name.endswith(".json"):
            file_path = os.path.join(DATA_PATH, file_name)
            with open(file_path, 'r') as file:
                postman_json = json.load(file)

    request_meta = {}
    for request in postman_json['item']:
        name = request['name']
        name = name.replace("https://online-go.com/", "")
        name = name.replace("https://cdn.online-go.com/", "")

        request_meta[name] = request['request']
        request_meta[name]["template"] = request['name']

    return request_meta


def get_data(request_meta):
    response_data_games = load_cached_data(RESPONSE_DATA_GAMES_PATH)
    response_data_demos = load_cached_data(RESPONSE_DATA_DEMOS_PATH)

    if not response_data_games:
        games_request_data = request_meta.get("api/v1/players/{{param1}}/games/")
        if games_request_data:
            response_data_games = get_paginated_data(games_request_data)
            if not response_data_games:
                return None, None
            
            save_data_to_cache(RESPONSE_DATA_GAMES_PATH, response_data_games)

    if not response_data_demos:
        demos_request_data = request_meta.get("api/v1/reviews/")
        if demos_request_data:
            response_data_demos = get_paginated_data(demos_request_data)
            if not response_data_demos:
                return None, None
            
            save_data_to_cache(RESPONSE_DATA_DEMOS_PATH, response_data_demos)

    return response_data_games, response_data_demos


def load_cached_data(cache_path):
    if os.path.exists(cache_path):
        with open(cache_path, 'r') as json_file:
            return json.load(json_file)
    return None


def save_data_to_cache(cache_path, data):
    with open(cache_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)


def get_paginated_data(request_data):
    url = request_data['url']['raw']
    headers = {header['key']: header['value'] for header in request_data['header']}
    aggregated_results = []
    next_url = url
    page_number = 1

    while next_url:
        response_data = fetch_request_data(next_url, headers)
        time.sleep(random.uniform(1.5, 3.0))
        if not response_data:
            return

        print(f"Page {page_number} retrieved.")
        aggregated_results.extend(response_data.get("results", []))
        next_url = response_data.get("next")
        page_number += 1

    return {"count": len(aggregated_results), "results": aggregated_results}


def fetch_request_data(url, headers, expect_json=True):
    tries = 0
    while tries < MAX_REQUEST_TRIES:
        tries += 1

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 429:
                print(f"Rate limit reached. Waiting for {RATE_LIMIT_DELAY} seconds.")
                time.sleep(RATE_LIMIT_DELAY)
                continue

            if not expect_json:
                return response.text if response.status_code == 200 else None

            response.raise_for_status()
            return response.json() if expect_json else None

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data from {url}: {e}")
            if tries >= MAX_REQUEST_TRIES:
                return None


def save_demo_urls(response_data_demos):
    with open(os.path.join(RESULTS_PATH, "demo_urls.txt"), 'w') as file:
        i = 0
        for match in response_data_demos.get("results", []):
            if match.get("game", {}).get("id", 0) != 0:
                continue

            url = f"https://online-go.com/review/{match['id']}"

            date_str = match.get("started")
            if not date_str:
                date_str = match.get("created")

            date = parser.parse(date_str)
            date_str = date.strftime("%d-%m-%y")
            weekday = date.strftime("%A")

            line = f"{url} {date_str}"
            if weekday in ["Wednesday", "Thursday"]:
                line += f" {weekday}"

            file.write(f"{line}\n")

            i += 1
            if i % 5 == 0:
                file.write("\n")


def load_processed_ids():
    if os.path.exists(PROCESSED_IDS_PATH):
        with open(PROCESSED_IDS_PATH, 'r') as file:
            return set(json.load(file))
    return set()


def save_processed_ids(processed_ids):
    with open(PROCESSED_IDS_PATH, 'w') as file:
        json.dump(list(processed_ids), file, indent=4)


def filter_and_download_matches(response_data, folder_path, request_meta, processed_ids, is_demos=False):
    matches = response_data.get("results", [])
    for i, match in enumerate(matches):
        if i > 0:
            processed_ids.add(matches[i-1]["id"])
            save_processed_ids(processed_ids)

        if match["id"] in processed_ids:
            continue

        if i + 1 == len(matches):
            processed_ids.add(match["id"])
            save_processed_ids(processed_ids)

        if not INCLUDE_CANCELLATIONS and \
            match.get("outcome") and match.get("outcome") == "Cancellation":
            continue
        if not INCLUDE_NON_19X19 and \
            ((match.get("width") and (match.get("width") != 19 or match.get("height") != 19)) or \
            (match.get("game", {}).get("width") and (match.get("game").get("width") != 19 or match.get("game").get("height") != 19))):
            continue
        if is_demos and not INCLUDE_REVIEWS and \
            match.get("game", {}).get("id", 0) != 0:
            continue

        result = download_and_save_sgf(match, folder_path, request_meta, is_demos)
        if not result:
            return False
        
        # break

    return True


def download_and_save_sgf(match, folder_path, request_meta, is_demos=False):
    request_data = request_meta.get("api/v1/games/{{param1}}/sgf")
    if is_demos:
        request_data = request_meta.get("api/v1/reviews/{{param1}}/sgf")

    url = request_data['template'].replace("{{param1}}", str(match["id"]))
    headers = {header['key']: header['value'] for header in request_data['header']}

    response = fetch_request_data(url, headers, expect_json=False)
    time.sleep(random.uniform(1.5, 3.0))
    if not response:
        return False

    move_count = response.count("W[") + response.count("B[")
    if move_count < MIN_MOVES:
        return True

    date_str = match.get("started")
    if not date_str:
        date_str = match.get("created")
    date_str = parser.parse(date_str).strftime("%y-%m-%d")

    tags = []

    b_player = replace_username(match["players"]["black"]["username"])
    w_player = replace_username(match["players"]["white"]["username"])
    player_str = f"{b_player} vs {w_player}"
    if b_player == "Black" or b_player == "" or b_player == USERNAME:
        player_str = f"vs {w_player}"
    elif w_player == "White" or w_player == "" or w_player == USERNAME:
        player_str = f"vs {b_player}"
    tags.append(player_str)

    match_name = match.get("name")
    if match_name:
        tags.append(match_name)

    handicap = 0
    if is_demos:
        handicap = response.count("AB[")
    else:
        handicap = match.get("handicap", 0)

        if match.get("ranked"):
            tags.append("ranked")
        else:
            tags.append("unranked")

        time_control_parameters = json.loads(match.get("time_control_parameters"))
        gamemode = time_control_parameters.get("speed")
        if gamemode:
            tags.append(gamemode)
    
    if handicap != 0:
        tags.append(f"handicap-{handicap}")

    filename = f"{date_str} {' - '.join(tags)}.sgf"
    filename = re.sub(r'[^a-zA-Z0-9_\-\.()&\' ]', '', filename)
    filepath = os.path.join(folder_path, filename)

    response = replace_username_in_sgf(response)

    with open(filepath, 'w', encoding='utf-8') as sgf_file:
        sgf_file.write(response)

    return True


def replace_username(username):
    for old_username in OLD_USERNAMES:
        if username in old_username:
            return USERNAME
    return username


def replace_username_in_sgf(sgf):
    for color in ["B", "W"]:
        element = f"P{color}[{USERNAME}]"
        for old_username in OLD_USERNAMES:
            sgf = sgf.replace(f"P{color}[{old_username}]", element)
    return sgf


if __name__ == "__main__":
    main()
