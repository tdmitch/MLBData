import dbfx
import mlbfx
import logging
import os
import utilfx
from datetime import datetime


season = 1988



""" 
    Get the list of games for the specified season
"""

# Set up logging
current_date = datetime.now().strftime('%Y%m%d')
log_file = os.path.join(os.getenv('LOGS_DIR', '.'), f'mlb_data_export_{current_date}.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

logging.info("Starting MLB data export for season %d", season)



logging.info("Retrieve and load list of games for season %d", season)

# Get the list of games for the specified season
games = mlbfx.getGameList(season)

# Truncate and reload the raw.Game table
dbfx.execute_non_query("TRUNCATE TABLE raw.Game")
dbfx.insert_rows('raw.Game', games)

# Load staged the data into the dbo.Game table
dbfx.execute_non_query("EXEC dbo.usp_Load_Game")


logging.info("Completed the load of games for season %d", season)




"""
    Get game details for each game in the list from above. Each file will be saved in the 
    GAMES_DOWNLOAD_DIR, and we'll parse the game details in a subsequent step.
"""

logging.info("Downloading game details for season %d games", season)

for game in games:
    # skip downloading detail for suspended, postponed, and cancelled games
    if game['detailedState'] != 'Suspended' and game['detailedState'] != 'Postponed' and game['detailedState'] != 'Cancelled':
        game_id = game['gameId']
    
        # Define the output directory
        output_dir = os.getenv('GAMES_DOWNLOAD_DIR')
        
        # Download the game details
        mlbfx.downloadGameDetail(game_id, output_dir)

logging.info("Completed the download of game files for season %d games", season)





"""
    Parse the game atBats and Pitches and load them into the database
"""

logging.info("Processing game details for season %d games from downloaded game files", season)

download_dir = os.getenv('GAMES_DOWNLOAD_DIR', '.')
for filename in os.listdir(download_dir):
    if filename.endswith('.json'):
        file_path = os.path.join(download_dir, filename)

        # Truncate the raw.AtBat and raw.Pitch tables to prepare for new data
        dbfx.execute_non_query("TRUNCATE TABLE raw.AtBat")
        dbfx.execute_non_query("TRUNCATE TABLE raw.Pitch")
                
        # Process each game detail file
        game_detail = mlbfx.getAtBats(file_path)

        if game_detail:
            # Insert the game detail into the staging table
            dbfx.insert_rows('raw.AtBat', game_detail)

            # Load the game detail into the main table
            dbfx.execute_non_query("EXEC dbo.usp_Load_AtBat")
            
        else:
            logging.warning("No game detail found for file: %s", filename)

        # Using the same file path, get the pitches for this game
        pitches = mlbfx.getPitches(file_path)

        if pitches:
            # Insert the pitches into the staging table
            dbfx.insert_rows('raw.Pitch', pitches)

            # Load the pitches into the main table
            dbfx.execute_non_query("EXEC dbo.usp_Load_Pitch")

        else:
            logging.warning("No pitches found for file: %s", filename)



# Move downloaded files to the archive directory
utilfx.move_files(source_dir=os.getenv('GAMES_DOWNLOAD_DIR'), 
                destination_dir=os.getenv('GAMES_ARCHIVE_DIR'), 
                extension=".json")
print(f"Moved downloaded files to the archive directory")
logging.info("Moved downloaded files to the archive directory")



logging.info("Completed processing of game details for season %d games", season)
