
# %%
import json
import pandas as pd
import numpy as np  
import re
from sqlalchemy import create_engine
from config import dbPassword
import psycopg2
import time
import challenge

fileDir="./data/"
wikiJsonFile = f"{fileDir}wikipedia.movies.json"
kaggleFile = f"{fileDir}movies_metadata.csv"
ratingsFile = f"{fileDir}ratings.csv"

challenge.moviesETLProcess(wikiJson=wikiJsonFile, kaggleCsv=kaggleFile , ratingCsv=ratingsFile)

