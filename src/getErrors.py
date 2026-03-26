from datetime import datetime
import utilfx
import os
import requests
import json


url = "https://statsapi.mlb.com/api/v1.1/game/745576/feed/live/"

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    errors = []

    all_plays = data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in all_plays:
        atBatIndex = play.get("atBatIndex")
        runners = play.get("runners", {})
        for runner in runners:
            if runner.get("details", {}).get("eventType", "").find("error") != -1:
                error_credits = runner.get("credits", {})
                for error_credit in error_credits:
                    if error_credit.get("credit").find("error") != -1:
                        err_type = error_credit['credit']
                        err_player = error_credit['player']['id']
                        err_position = error_credit['position']['code']
                        errors.append({
                            "game_id": data["gamePk"],
                            "inning": play.get("about", {}).get("inning"),
                            "at_bat_index": atBatIndex,
                            "error_type": err_type,
                            "player_id": err_player,
                            "position_code": err_position
                        })
    print()
    print(f"Found {len(errors)} errors in game {data['gamePk']}:")    
    print(errors)
else:
    print(f"Failed to fetch data: {response.status_code}")