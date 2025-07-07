import dbfx
import mlbfx
import logging
import os
from datetime import datetime


season = 2025




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

        # From the same file path, get the pitches
        pitches = mlbfx.getPitches(file_path)

        if pitches:
            # Insert the pitches into the staging table
            dbfx.insert_rows('raw.Pitch', pitches)

            # Load the pitches into the main table
            dbfx.execute_non_query("EXEC dbo.usp_Load_Pitch")

        else:
            logging.warning("No pitches found for file: %s", filename)



logging.info("Completed processing of game details for season %d games", season)
