import dbfx
import mlbfx
import logging
import os
import utilfx
from datetime import datetime


# Load game types
def load_game_types():
    
    game_type = mlbfx.getGameTypes()

    if (game_type):
        dbfx.execute_non_query("TRUNCATE TABLE raw.GameType")
        dbfx.insert_rows('raw.GameType', game_type)
        dbfx.execute_non_query("EXEC dbo.usp_Load_GameType")


# Load pitch types
def load_pitch_types():

    pitch_type = mlbfx.getPitchTypes()

    if (pitch_type):
        dbfx.execute_non_query("TRUNCATE TABLE raw.PitchType")
        dbfx.insert_rows('raw.PitchType', pitch_type)
        dbfx.execute_non_query("EXEC dbo.usp_Load_PitchType")


# Load positions
def load_positions():
    
    positions = mlbfx.getPositions()

    if (positions):
        dbfx.execute_non_query("TRUNCATE TABLE raw.Position")
        dbfx.insert_rows('raw.Position', positions)
        dbfx.execute_non_query("EXEC dbo.usp_Load_Position")


if __name__ == "__main__":
    load_pitch_types()  
    load_game_types() 
    load_positions()  