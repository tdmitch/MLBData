import requests
from datetime import datetime, timedelta
import json



def getGameList(season):
    """
        This function retrieves a list of games for a given MLB season. It loops through
        all potential dates for the season, checking each date for scheduled games.
        If games are found, it extracts relevant details and returns a list of game dictionaries.

        :param season: The MLB season year (e.g., 2025)
        :return: A list of dictionaries, each containing details about a game.
        
        Example dictionary structure:
        {
            'season': 2025,
            'gameId': 123456,
            'gameType': 'R',
            'doubleHeader': 'N',
            'gamedayType': 'S',
            'tiebreaker': 'N',
            'dayNight': 'D',
            'gamesInSeries': 1,
            'seriesGameNumber': 1,
            'gameDateTime': '2025-04-01T19:05:00Z',
            'homeTeam': 123,
            'awayTeam': 456,
            'venue': 789,
            'reason': None,
            'detailedState': None
        }

    """

    # Game list is stored by date, so we need the min and max dates for the season.
    # These variables are set to stretch beyond any potential game dates for the 
    # selected season.
    season_min_date = f"{season}-02-01"
    season_max_date = f"{season}-11-30"    
    
    # convert the start date and end date to date time value
    first_game_date = datetime.strptime(season_min_date,'%Y-%m-%d')
    last_game_date = datetime.strptime(season_max_date,'%Y-%m-%d')
    

    # List of games for the current date
    games = []
    
    # Loop through potential game dates    
    game_date = first_game_date
    while game_date <= last_game_date:
        game_date = game_date.strftime("%m/%d/%Y")
        
        # API call for current date
        scheduleRequestString = f"http://statsapi.mlb.com/api/v1/schedule/games/?sportId=1&date={game_date}"
        schedule = requests.get(scheduleRequestString).json()
        
        # Loop through games on this date if any and add to the games list
        if schedule['totalGames'] > 0:
            for game in schedule['dates'][0]['games']:
                gameDetails = {}
                gameDetails['season'] = season
                gameDetails['gameId'] = game['gamePk']
                gameDetails['gameType'] = game['gameType']
                gameDetails['doubleHeader'] = game['doubleHeader']
                gameDetails['gamedayType'] = game['gamedayType']
                gameDetails['tiebreaker'] = game['tiebreaker']
                gameDetails['dayNight'] = game['dayNight']
                gameDetails['gamesInSeries'] = game.get('gamesInSeries', 'NULL')
                gameDetails['seriesGameNumber'] = game.get('seriesGameNumber', 'NULL')
                gameDetails['gameDateTime'] = game['gameDate']                
                gameDetails['homeTeam'] = game['teams']['home']['team']['id']
                gameDetails['awayTeam'] = game['teams']['away']['team']['id']
                gameDetails['venue'] = (game['venue']['id'])                
                if game.get('status', None) is not None:
                    gameDetails['reason'] = game['status'].get('status', 'NULL')
                    gameDetails['detailedState'] = game['status'].get('detailedState', 'NULL')

                # Add this game to the list
                games.append(gameDetails)

        # Increment the game date by one day
        game_date = datetime.strptime(game_date, "%m/%d/%Y") + timedelta(days=1)

    return games




def downloadGameDetail(game_id, output_dir):
    """
        This function downloads the game details for a specific game ID and saves it to a JSON file.
        
        :param game_id: The unique identifier for the MLB game.
        :param output_dir: The directory where the game details JSON file will be saved.
    """
    # Build the URL for this game and get game data
    game_url = f'https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live/'
    game_data = requests.get(game_url).json()

    game_id_string = game_data["gameData"]["game"]["id"].replace("/", "_").replace("-", "_")
    
    if not output_dir.endswith("\\"):
        output_dir += "\\"
    output_dir += f"{ game_id_string }.json"

    # Save the game feed to a file
    with open(output_dir, 'w', encoding='utf-8') as f:
        f.write(str(json.dumps(game_data)))  




def getAtBats(file_path):

    atBats = []
    game_data = None

    with open(file_path, 'r', encoding='utf-8') as f:
        game_data = json.load(f) 
    
    gameId = game_data['gamePk']
    
    # Get the AtBats from this file
    for atBat in game_data['liveData']['plays']['allPlays']:
        if atBat.get('result', None) is not None and atBat['result'].get('type', None) == 'atBat':
            atBatValues = {}
            atBatValues['gameId']                  = gameId
            atBatValues['pitcherId']               = atBat['matchup']['pitcher']['id'] 
            atBatValues['pitchHand']               = atBat['matchup']['pitchHand']['code']
            atBatValues['batterId']                = atBat['matchup']['batter']['id']
            atBatValues['batSide']                 = atBat['matchup']['batSide']['code']
            atBatValues['atBatIndex']              = atBat['atBatIndex']

            atBatValues['halfInning']              = atBat['about'].get('halfInning', 'NULL')
            atBatValues['inning']                  = atBat['about'].get('inning', 'NULL')
            atBatValues['startTime']               = atBat['about'].get('startTime', 'NULL')
            atBatValues['endTime']                 = atBat['about'].get('endTime', 'NULL')
            atBatValues['isScoringPlay']           = atBat['about'].get('isScoringPlay', 'NULL')
            atBatValues['hasOut']                  = atBat['about'].get('hasOut', 'NULL')
            atBatValues['hasReview']               = atBat['about'].get('hasReview', 'NULL')

            atBatValues['resultType']              = atBat['result'].get('type', 'NULL')
            atBatValues['event']                   = atBat['result'].get('event', 'NULL')
            atBatValues['eventType']               = atBat['result'].get('eventType', 'NULL')
            atBatValues['rbi']                     = atBat['result'].get('rbi', 'NULL')
            atBatValues['awayScore']               = atBat['result'].get('awayScore', 'NULL')
            atBatValues['homeScore']               = atBat['result'].get('homeScore', 'NULL')
            atBatValues['isComplete']              = atBat['result'].get('isComplete', 'NULL')

            # Add this At Bat to the list
            atBats.append(atBatValues)


    return atBats



def getPitches(file_path):
    """
        This function retrieves pitch data from a game detail file. We're going to reuse the logic
        from getAtBats to parse the file, and use the pitches if present in the atBats data
        
        :param file_path: The path to the game detail JSON file.
        :return: A list of dictionaries, each containing pitch details.
    """

    pitches = []
    game_data = None

    with open(file_path, 'r', encoding='utf-8') as f:
        game_data = json.load(f) 
    
    gameId = game_data['gamePk']
    
    # Get the AtBats from this file
    for atBat in game_data['liveData']['plays']['allPlays']:
        if atBat.get('result', None) is not None and atBat['result'].get('type', None) == 'atBat':
            for pitch in atBat['playEvents']:
                if pitch['isPitch']:
                # Store pitch values in a dictionary

                    pitchValues = {}     
                    pitchValues['gameId']              = gameId
                    pitchValues['atBatIndex']          = atBat['atBatIndex']
                    pitchValues['pitcherId']           = atBat['matchup']['pitcher']['id']
                    pitchValues['batterId']            = atBat['matchup']['batter']['id']
                    pitchValues['playId']              = pitch['playId']
                    pitchValues['pitchNumber']         = pitch['pitchNumber']
                    pitchValues['isInPlay']            = pitch['details']['isInPlay']
                    pitchValues['isStrike']            = pitch['details']['isStrike']
                    pitchValues['isBall']              = pitch['details']['isBall']
                    pitchValues['callCode']            = pitch['details']['call']['code']
                    pitchValues['typeCode']            = pitch['details'].get('type', {}).get('code', None)
                    pitchValues['countBalls']          = pitch['count']['balls']
                    pitchValues['countStrikes']        = pitch['count']['strikes']
                    
                    # pitchData
                    pitchData = pitch.get('pitchData', None)
                    if pitchData is not None:                       
                        pitchValues['startSpeed']          = pitchData.get('startSpeed', 'NULL')
                        pitchValues['endSpeed']            = pitchData.get('endSpeed', 'NULL')
                        pitchValues['strikeZoneTop']       = pitchData.get('strikeZoneTop', 'NULL')
                        pitchValues['strikeZoneBottom']    = pitchData.get('strikeZoneBottom', 'NULL')
                        pitchValues['zone']                = pitchData.get('zone', 'NULL')
                        pitchValues['plateTime']           = pitchData.get('plateTime', 'NULL')
                        
                        
                        # coordinates
                        coordinates         = pitchData.get('coordinates', None)
                        if coordinates is not None:                            
                            pitchValues['aY']                  = coordinates.get('aY', 'NULL')
                            pitchValues['aZ']                  = coordinates.get('aZ', 'NULL')
                            pitchValues['pfxX']                = coordinates.get('pfxX', 'NULL')
                            pitchValues['pfxZ']                = coordinates.get('pfxZ', 'NULL')
                            pitchValues['pX']                  = coordinates.get('pX', 'NULL')
                            pitchValues['pZ']                  = coordinates.get('pZ', 'NULL')
                            pitchValues['vX0']                 = coordinates.get('vX0', 'NULL')
                            pitchValues['vY0']                 = coordinates.get('vY0', 'NULL')
                            pitchValues['vZ0']                 = coordinates.get('vZ0', 'NULL')
                            pitchValues['x']                   = coordinates.get('x', 'NULL') 
                            pitchValues['y']                   = coordinates.get('y', 'NULL') 
                            pitchValues['x0']                  = coordinates.get('x0', 'NULL')
                            pitchValues['y0']                  = coordinates.get('y0', 'NULL')
                            pitchValues['z0']                  = coordinates.get('z0', 'NULL')
                            pitchValues['aX']                  = coordinates.get('aX', 'NULL')
                        

                        # breaks                                               
                        breaks = pitchData.get('breaks', None)
                        if breaks is not None:
                            pitchValues['breakAngle']          = breaks.get('breakAngle', 'NULL')
                            pitchValues['breakLength']         = breaks.get('breakLength', 'NULL')
                            pitchValues['breakY']              = breaks.get('breakY', 'NULL')
                            pitchValues['spinRate']            = breaks.get('spinRate', 'NULL')
                            pitchValues['spinDirection']       = breaks.get('spinDirection', 'NULL')
                            
                        
                        hitData = pitchData.get('hitData', None)
                        if hitData is not None:
                            # hit data
                            pitchValues['launchSpeed']         = hitData.get('launchSpeed', 'NULL')
                            pitchValues['launchAngle']         = hitData.get('launchAngle', 'NULL')
                            pitchValues['totalDistance']       = hitData.get('totalDistance', 'NULL')
                            pitchValues['trajectory']          = hitData.get('trajectory', 'NULL')
                            pitchValues['hardness']            = hitData.get('hardness', 'NULL')
                            pitchValues['location']            = hitData.get('location', 'NULL')
                            
                            # hit coordinates 
                            hitDataCoordinates = hitData.get('coordinates', None)
                            if hitDataCoordinates is not None:
                                pitchValues['coordX']              = hitDataCoordinates.get('coordX', 'NULL')
                                pitchValues['coordY']              = hitDataCoordinates.get('coordY', 'NULL')

                    ############################################################################################################################################

                    pitches.append(pitchValues)

    return pitches