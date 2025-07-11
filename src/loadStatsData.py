import dbfx
import mlbfx
import logging
import os
import utilfx
from datetime import datetime
import time



season = 1958
season_stop = 1935


while season >= season_stop:

    GAMES_DOWNLOAD_DIR = os.getenv('GAMES_DOWNLOAD_DIR')


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

    logging.info(f"**** Starting MLB data export for the { season } season ****")
    print(f"**** Starting MLB data export for the { season } season ****")



    logging.info(f"Retrieve and load list of games for the { season } season")
    print(f"Retrieve and load list of games for the { season } season")

    # Get the list of games for the specified season
    games = mlbfx.getGameList(season)
    if games is None:
        logging.error(f"Failed to retrieve game list for the { season } season. Exiting.")
        print(f"Failed to retrieve game list for the { season } season. Exiting.")
        break

    # Truncate and reload the raw.Game table
    dbfx.execute_non_query("TRUNCATE TABLE raw.Game")
    dbfx.insert_rows('raw.Game', games)

    # Load staged the data into the dbo.Game table
    dbfx.execute_non_query("EXEC dbo.usp_Load_Game")


    

    """
        Get game details for each game in the list from above. Each file will be saved in the 
        GAMES_DOWNLOAD_DIR, and we'll parse the game details in a subsequent step.
    """

    logging.info(f"Downloading game detail files for the { season } season")
    print(f"Downloading game detail files for the { season } season")

    
    file_error = False
    for game in games:
        # skip downloading detail for suspended, postponed, and cancelled games
        if game['detailedState'] != 'Suspended' and game['detailedState'] != 'Postponed' and game['detailedState'] != 'Cancelled':
            game_id = game['gameId']
        
            # Download the game details
            success = mlbfx.downloadGameDetail(game_id, GAMES_DOWNLOAD_DIR)
            if not success:
                logging.error(f"Failed to download game detail for game ID: { game_id }")
                print(f"Failed to download game detail for game ID: { game_id }")
                file_error = True
                break  # Exit the loop if any file download fails

            time.sleep(1)  # Sleep for 1 second to avoid overwhelming the API

    if file_error:
        logging.error("One or more game detail files failed to download. Exiting.")
        print("One or more game detail files failed to download. Exiting.")
        break




    """
        Parse the game atBats and Pitches and load them into the database
    """

    logging.info(f"Processing game details for the { season } season from downloaded game files")
    print(f"Processing game details for the { season } season from downloaded game files")

    for filename in os.listdir(GAMES_DOWNLOAD_DIR):
        if filename.endswith('.json'):
            file_path = os.path.join(GAMES_DOWNLOAD_DIR, filename)

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
                logging.warning(f"No game detail found for file: { filename }")

            # Using the same file path, get the pitches for this game
            pitches = mlbfx.getPitches(file_path)

            if pitches:
                # Insert the pitches into the staging table
                dbfx.insert_rows('raw.Pitch', pitches)

                # Load the pitches into the main table
                dbfx.execute_non_query("EXEC dbo.usp_Load_Pitch")

            else:
                logging.warning(f"No pitches found for file: { filename }")





    """
        Move the game files to the archive directory, and create a zip archive of the entire season.
        After creating the zip file, delete the original JSON files.
    """

    logging.info("Moving downloaded files to the archive directory")
    print(f"Moving downloaded files to the archive directory")

    GAMES_ARCHIVE_DIR = os.getenv('GAMES_ARCHIVE_DIR')


    # Move downloaded files to the archive directory
    utilfx.move_files(source_dir = GAMES_DOWNLOAD_DIR, 
                    destination_dir = GAMES_ARCHIVE_DIR, 
                    extension = ".json")
    
    

    logging.info("Creating archive file, and deleting original JSON files.")
    print("Creating archive file, and deleting original JSON files.")



    # Create a zip archive of the game detail files for the season, and delete the original JSON files.
    archive_dir = GAMES_ARCHIVE_DIR
    file_pattern = str(season) + '*.json'
    archive_filename = os.path.join(archive_dir, "game_detail_" + str(season) + ".zip")

    utilfx.archive_files(archive_dir, file_pattern, archive_filename)

    

    logging.info(f"**** Completed processing of game details for the { season } season ****")
    print(f"\n**** Completed processing of game details for the { season } season ****\n")

    season -= 1

    # Sleep to avoid overwhelming the API with requests
    sleep_duration_minutes = 1
    
    logging.info(f"Sleeping for { sleep_duration_minutes } minutes to avoid getting locked out of the API")
    print(f"Sleeping for { sleep_duration_minutes  } minutes to avoid getting locked out of the API\n\n")

    time.sleep((sleep_duration_minutes * 60))
