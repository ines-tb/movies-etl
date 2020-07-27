# Movies ETL

ETL to Extract, Transform and Load data from movies and the ratings given in Kaggle


#### Overview
Amazing Prime wants to proccess different files with movies information collected from wikipedia and Kaggle and the the rates from many users provided also by the latter. Additionally, it is required to load the information in a database to make it available to be queried. An automated function is provided to extract, transform and load the data.

###### Resources
* Data Sources: _wikipedia.movies.json_, _movies_metadata.csv_ and _ratings.csv_.
* Software: Python 3.7.7, PostgreSQL 11, pgAdmin 4, Visual Studio Code 1.45.1.
---

#### Assumptions
For this ETL process several assumptions have been made based on current data.

* Input Wikipedia data will be a json file, while Kaggle data will be both csv files.
  
* In wikipedia data there will be a field called imbd_link that will contain an 'imdb_id' with format '_ttddddddddd_' where d is a digit number. That same field named imdb_id will be present in Kaggle metadata.
  
* The following columns are expected:
  - In Wikipedia file at least: imdb_link, Box office, Budget, Release date, Running time,title_wiki,release_date_wiki,Language and Production company(s).
    
  - In Kaggle Metadata at least: imdb_id, adult, video, budget, id, popularity, release_date, title, runtime, budget,revenue,release_date, original_language and production_companies.
  
  - Among Wikipedia and Kaggle there will be: tagline,belongs_to_collection,url,runtime,revenue,popularity,vote_average,vote_count,genres,original_language,overview,spoken_languages,Country,production_companies,production_countries,Distributor,Producer(s),Director,Starring,Cinematography,Editor(s),Writer(s),Composer(s) and Based on.
  
  - In Ratings file at least: movieId,rating, userId and timestamp.

* When merging Wikipedia and Kaggle data, as some columns are present in both data sources the Kaggle data will be chosen over the wikipedia for Title, Release date, Original language and Production companies. For Runtime, Budget and Revenue, Kaggle data will be selected unless no value found which will be filled with wikipedia data.
* Kaggle file columns will be as follows: 'adult' column will be strings either "True" or "False"; budget, id and popularity will be convertible to numeric types and release_date to date type. 
* Ratings file columns will have the following types: userId -> int64, movieId  -> int64, rating -> float64 and timestamp  -> int64. Being the timestamp a valid value to be converted into date.
  
_NOTE_: The process will only consider entries where column 'adult' in Kaggle Metadata has value False.
