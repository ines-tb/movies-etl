#%%
import json
import pandas as pd
import numpy as np  
import re
from sqlalchemy import create_engine
from config import dbPassword
import psycopg2
import time


def moviesETLProcess(wikiJson: str, kaggleCsv:str, ratingCsv:str):
    
    # Auxiliar functions
    # -------------------------
    
    def readJsonFile(jsonPath) -> list:
        with open(jsonPath, mode="r") as file:
            # Return the json content as a list
            return json.load(file)

    def readCsvFile(csvPath, low_memory = True):
        return pd.read_csv(csvPath, low_memory = low_memory)

    print("ETL Starts...\n**************************\n")

    # *****************************************
    #           Extraction phase              
    # *****************************************
    print("Extraction step...")
    try:
        # Wiki data
        wikiMoviesRaw = readJsonFile(wikiJson)
        
        # Kaggle data
        kaggleMetadata = readCsvFile(kaggleCsv, low_memory=False)
        ratings = readCsvFile(ratingCsv)

    except FileNotFoundError as fnf:
        print(f"\nError 'File not found' raised while loading date: {fnf}\n\n/!\ Aborting ETL process.")
        return
    except Exception as e:
        print(f"\nError raised while loading files: {e}\n\n/!\ Aborting ETL process.")
        return


    # *****************************************
    #           Transform phase              
    # *****************************************
    
    print("Transformation step...")

    wikiMoviesDF = pd.DataFrame(wikiMoviesRaw)

    # Filter movies with directors and imbd_id and not "No. of episodes" (for TV shows)
    wikiMovies = [movie for movie in wikiMoviesRaw 
        if ("Director" in movie or "Directed by" in movie) 
            and "imdb_link" in movie
            and 'No. of episodes' not in movie]
    
    wikiMoviesDF = pd.DataFrame(wikiMovies)

    # Function to clean alternative titles and columns refferring same content from each movie
    def cleanMovie(movie):
        movie = dict(movie) # create a non-destructive copy
        altTitles = {}
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                altTitles[key]=movie[key]
                movie.pop(key)
        
        if len(altTitles) > 0:
            movie["alt_titles"] = altTitles
        
        # Function to change the name of a column
        def changeColumnName(oldName, newName):
            if oldName in movie:
                movie[newName] = movie.pop(oldName)

        # Change the name columns with different name but same data content  
        changeColumnName('Adaptation by', 'Writer(s)')
        changeColumnName('Country of origin', 'Country')          
        changeColumnName("Directed by", "Director")
        changeColumnName("Distributed by", "Distributor")
        changeColumnName("Edited by", "Editor(s)")
        changeColumnName('Length', 'Running time')
        changeColumnName('Original release', 'Release date')
        changeColumnName('Music by', 'Composer(s)')
        changeColumnName("Produced by", "Producer(s)")
        changeColumnName("Producer", "Producer(s)")
        changeColumnName('Productioncompanies ', 'Production company(s)')
        changeColumnName('Productioncompany ', 'Production company(s)')
        changeColumnName("Released", "Release date")
        changeColumnName('Screen story by', 'Writer(s)')
        changeColumnName('Screenplay by', 'Writer(s)')
        changeColumnName('Story by', 'Writer(s)')
        changeColumnName('Theme music composer', 'Composer(s)')
        changeColumnName('Written by', 'Writer(s)')

        return movie


    # Create cleanMovies list
    cleanMovies = [cleanMovie(movie) for movie in wikiMovies]
    # Dataframe with cleaned data
    wikiMoviesDF = pd.DataFrame(cleanMovies)

    # Clean duplicate rows
    wikiMoviesDF["imdb_id"] = wikiMoviesDF["imdb_link"].str.extract(r"(tt\d{7})")
    wikiMoviesDF.drop_duplicates(subset="imdb_id", inplace=True)

    # Clean mostly null columns
    # --------------------------------
    # Get columns with non-null values > 90%
    wikiColumnsToKeep = [column for column in wikiMoviesDF.columns if wikiMoviesDF[column].isnull().sum() < len(wikiMoviesDF) * 0.9]
    # Redefine the Dataframe to only keep the wanted columns 
    wikiMoviesDF = wikiMoviesDF[wikiColumnsToKeep]


    # ---------------------------
    # Convert and parse data
    # ---------------------------
  
    # Auxiliar functions

    # Drop null values and Covert any list to string
    def dropNullsListToString(column) -> pd.Series:
        return column.dropna().apply(lambda x: " ".join(x) if type(x) == list else x)

    # Function to convert currency values into numeric format
    def parseDollars(s):

        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r"\$\s*\d+\.?\d*\s*milli?on", s, flags=re.IGNORECASE):
            
            # remove dollar sign and " million"
            s = re.sub(r"\$|\s|[a-zA-Z]", "", s)
            # convert to float and multiply by a million
            value = float(s) * 10**6
            return value
            
        # if input is of the form $###.# billion
        elif re.match(r"\$\s*\d+\.?\d*\s*billi?on", s, flags=re.IGNORECASE):
            
            # remove dollar sign and " billion"
            s = re.sub(r"\$|\s|[a-zA-Z]", "", s)
            # convert to float and multiply by a billion
            value = float(s) * 10**9
            return value
        
        # if input is of the form $###,###,###
        elif re.match(r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)", s, flags=re.IGNORECASE):
            
            # remove dollar sign and commas
            s = re.sub(r'\$|,','', s)
            # convert to float
            value = float(s)
            return value
            
        else:        
            return np.nan
    
    # Assign value to new column applying function and drop old column
    def newColumnFromOld(df, newColumn, oldColumn, series, pattern, function) -> pd.DataFrame:
        # Apply the function and assign to a new column in dataframe
        df[newColumn] = series.str.extract(pattern, flags=re.IGNORECASE)[0].apply(function)
        # Drop original column
        df.drop(oldColumn, axis=1, inplace=True)
        return df
    
    # Convert BoxOffice to numerical
    # -------------------------------
    
    # Drop null values and convert lists to strings
    boxOffice = dropNullsListToString(wikiMoviesDF["Box office"])
    # Patterns to match    
    formOne = r"\$\s*\d+\.?\d*\s*[mb]illi?on"
    formTwo = r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)"
    # Handle ranges: just keep the second number of the range
    boxOffice = boxOffice.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    # Apply the function and assign to a new column in wikiMoviesDF and drop 'Box office' column
    wikiMoviesDF = newColumnFromOld(df=wikiMoviesDF,newColumn="box_office",oldColumn="Box office",series=boxOffice,pattern=f"({formOne}|{formTwo})",function=parseDollars)


    # Convert Budget to numerical:
    # -----------------------------

    # Drop null values and convert lists to strings
    budget = dropNullsListToString(wikiMoviesDF["Budget"])
    # Keep last value when given ranges
    budget = budget.str.replace(r"\$.*[-—–](?![a-z])", "$", regex=True)
    # Eliminate citations (number in square brackets)        
    budget = budget.str.replace(r"\[\d+\]\s*", "")    
    # Apply the function, assign to a new column in wikiMoviesDF and drop 'Budget' column
    wikiMoviesDF = newColumnFromOld(df=wikiMoviesDF,newColumn="budget",oldColumn="Budget",series=budget,pattern=f"({formOne}|{formTwo})",function=parseDollars)



    # Convert Release date to date object:
    # -------------------------------------

    # Drop null values and Covert any list to string
    releaseDate = dropNullsListToString(wikiMoviesDF["Release date"])
    # Regex to match Month dd, yyyy
    dateFormOne = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}"
    # Regex to match yyyy.mm.dd where . can be any separator
    dateFormTwo = r"\d{4}.[01]\d.[123]\d"
    # Regex to match Month yyyy
    dateFormThree = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}"
    # Regex to match yyyy
    dateFormFour = r"\d{4}"
    try:
        # Create column to hold the matching dates using the built-in pandas date function to_datetime
        wikiMoviesDF["release_date"] = pd.to_datetime(releaseDate.str.extract(f"({dateFormOne}|{dateFormTwo}|{dateFormThree}|{dateFormFour})")[0], infer_datetime_format=True)
    except Exception as e:
        print(f"\nError raised while parsing 'release_date' from \wikipedia data: {e}\n\n/!\ Aborting ETL process.")
        return


    # Convert Running time to numerical:
    # ----------------------------------

    try:
        # Drop null values and Covert any list to string
        runningTime = dropNullsListToString(wikiMoviesDF["Running time"])
        runningTimeExtract = runningTime.str.extract(r"(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m")
        # Convert to numbers. coerce will turn empty strings to NaN.
        runningTimeExtract = runningTimeExtract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        wikiMoviesDF["running_time"] = runningTimeExtract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        # Drop original 'Running time' column
        wikiMoviesDF.drop('Running time', axis=1, inplace=True)
    except Exception as e:
        print(f"\nError raised while parsing 'Running time' from wikipedia data: {e}\n\n/!\ Aborting ETL process.")
        return

    # ---------------------------
    # Clean Kaggle Data
    # ---------------------------
    
    # Adult column: For our study we only keep movies where Adult = False
    kaggleMetadata = kaggleMetadata[kaggleMetadata["adult"] == "False"].drop("adult", axis="columns")

    # Video Column: Assign the values to the column itself to get the boolean values
    kaggleMetadata["video"] = kaggleMetadata["video"] == "True"


    # Convert to numeric Budget, id and property:
    try:
        kaggleMetadata["budget"] = kaggleMetadata["budget"].astype(int)
        kaggleMetadata["id"] = pd.to_numeric(kaggleMetadata["id"],errors="raise")
        kaggleMetadata["popularity"] = pd.to_numeric(kaggleMetadata["popularity"],errors="raise")
    except Exception as e:
        print(f"\nError raised while parsing Kaggle Metadata: {e}\n\n/!\ Aborting ETL process.")
        return

    # Convert release_date to dates:
    try:
        kaggleMetadata["release_date"] = pd.to_datetime(kaggleMetadata["release_date"])
    except Exception as e:
        print(f"\nError raised while parsing 'release_date' from Kaggle \Metadata: {e}\n\n/!\ Aborting ETL process.")
        return

    # ---------------------------
    # Clean Ratings Data
    # ---------------------------

    try:        
        ratings["timestamp"] = pd.to_datetime(ratings["timestamp"], unit="s")
    except Exception as e:
        print(f"\nError raised while parsing 'timestamp' from Kaggle \Metadata: {e}\n\n/!\ Aborting ETL process.")
        return
        
    # ---------------------------
    # Merge wiki and Kaggle data
    # ----------------------------

    moviesDF = pd.merge(wikiMoviesDF, kaggleMetadata, on="imdb_id", suffixes=["_wiki","_kaggle"])

    
    # Remove known outlier:
    # Drop the element with index where relase date wildly differ
    moviesDF = moviesDF.drop(moviesDF[(moviesDF["release_date_wiki"]>'1996-01-01') & (moviesDF["release_date_kaggle"]<'1965-01-01')].index)
    
    # Competing data:
    # Wiki                     Movielens                Resolution
    #--------------------------------------------------------------------------
    # title_wiki               title_kaggle             Drop wikipedia
    # running_time             runtime                  Keep Kaggle; fill in zeros with Wikipedia data.
    # budget_wiki              budget_kaggle            Keep Kaggle; fill in zeros with Wikipedia data.
    # box_office               revenue                  Keep Kaggle; fill in zeros with Wikipedia data.
    # release_date_wiki        release_date_kaggle      Drop wikipedia
    # Language                 original_language        Drop wikipedia
    # Production company(s)    production_companies     Drop wikipedia

    # Execute the resolutions 
    # ----------------------------

    # Drop wiki columns for title, release date, language and production company
    moviesDF.drop(columns=["title_wiki","release_date_wiki","Language","Production company(s)"], inplace=True)

    # Function to fill missing data from a column and drop the redudandant column
    def fillMissingKaggleData (df, kaggleColumn, wikiColumn):
        df[kaggleColumn] = df.apply(lambda row: row[wikiColumn] if row[kaggleColumn] == 0 else row[kaggleColumn], axis=1)
        df.drop(columns=wikiColumn,inplace=True)

    # Apply the function for runtime, budget and revenue
    fillMissingKaggleData(df=moviesDF, kaggleColumn="runtime", wikiColumn="running_time")
    fillMissingKaggleData(df=moviesDF, kaggleColumn="budget_kaggle", wikiColumn="budget_wiki")
    fillMissingKaggleData(df=moviesDF, kaggleColumn="revenue", wikiColumn="box_office")

    # Reorder columns to make it easier to read
    moviesDF = moviesDF.loc[:, ["imdb_id","id","title_kaggle","original_title","tagline","belongs_to_collection","url","imdb_link","runtime","budget_kaggle","revenue","release_date_kaggle","popularity","vote_average","vote_count","genres","original_language","overview","spoken_languages","Country","production_companies","production_countries","Distributor","Producer(s)","Director","Starring","Cinematography","Editor(s)","Writer(s)","Composer(s)","Based on"]]

    # Rename columns 
    moviesDF.rename({'id':'kaggle_id',
                    'title_kaggle':'title',
                    'url':'wikipedia_url',
                    'budget_kaggle':'budget',
                    'release_date_kaggle':'release_date',
                    'Country':'country',
                    'Distributor':'distributor',
                    'Producer(s)':'producers',
                    'Director':'director',
                    'Starring':'starring',
                    'Cinematography':'cinematography',
                    'Editor(s)':'editors',
                    'Writer(s)':'writers',
                    'Composer(s)':'composers',
                    'Based on':'based_on'
                    }, axis='columns', inplace=True)

    # ----------------------------
    # Merge wiki and Ratings data
    # ----------------------------

    # Group by movieId and rating, rename column as count 
    #       and pivot the data so movieId is the index, columns the ratings and the rows the counts for each rating 
    ratingCounts = ratings.groupby(["movieId", "rating"],as_index=False).count().rename({"userId":"count"},axis=1).pivot(index="movieId",columns="rating", values="count")

    # Prepend the string 'rating_' to the columns 
    ratingCounts.columns = ["rating_" + str(col) for col in ratingCounts.columns]

    # Merge with a left merge into movieDF
    moviesWithRatingsDF = pd.merge(moviesDF, ratingCounts, left_on="kaggle_id", right_index=True, how="left")

    # Fill missing values for a rating-movie with zeros
    moviesWithRatingsDF[ratingCounts.columns] = moviesWithRatingsDF[ratingCounts.columns].fillna(0)

    # *****************************************
    #              Load phase              
    # *****************************************
    
    print("Loading step...")

    # Create the database engine
    dbString = f"postgres://postgres:{dbPassword}@127.0.0.1:5432/movie_data"
    engine = create_engine(dbString)

    try:
        
        # Load movies Data into DB
        moviesWithRatingsDF.to_sql(name="movies_cha", con=engine, if_exists="replace")
        print("   > Movies data successfully loaded into DB.")
    except Exception as e:
        print(f"\nError raised loading data into movies table: {e}\n\nMovies data not loaded or loaded with errors.")
        return 
    
    try:        
        # Load ratings Data into DB
        # It will be done by chunks due to its high volume

        # Rows imported variable
        rowsImported = 0
        # Start time variable 
        startTime = time.time()

        for data in pd.read_csv(ratingCsv, chunksize=1000000):
        
            print(f"importing rows {rowsImported} to {rowsImported + len(data)}...", end="")
            
            if rowsImported == 0:
                
                data.to_sql(name="ratings_cha", con=engine, if_exists="replace")
            else:
                
                data.to_sql(name="ratings_cha", con=engine, if_exists="append")

            # increment the number of rows imported by the chunksize
            rowsImported += len(data)
            
            # End of processing and elapsed time        
            print(f"Done. {time.time() - startTime} total seconds elapsed.")

        print("   > Ratings data successfully loaded into DB.")

    except Exception as e:
        print(f"\nError raised loading data into ratings table: {e}\n\nRatings data not loaded or loaded with errors.")
        return 
    
    print("\n**************************\nETL successfully finished.")

