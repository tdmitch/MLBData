import dbfx
import mlbfx



season = 1997

games = mlbfx.getGameList(season)

dbfx.insert_rows('raw.Game', games)