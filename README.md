# moviedb.teamplate
This template shows example usage of the Metis Machine platform for the purpose of data ingestion and curation. Fundamentally, the task is to go out each morning and fetch a list of valid movie IDs from www.themoviebd.org (TMDb) and then retrieve additional data about each film (genres, release date, length, etc).

## Dependencies
1. User must sign up and aqcuire a free API key from TMDb.
  - Register Here --> https://www.themoviedb.org/account/signup
2. Set the API key as an environment variable with the skafos CLI.
  - Run from the terminal (in your project directory): `skafos env API_KEY --set <API KEY>`

## Project Structure
- *movies*
  - `__init__.py`
  - `constants.py`
  - `logger.py`
  - `movie_fetch.py`
  - `movie_info.py`
- *metis.config.yml*
- *environment.yml*
- *README.md*
- *main.py*

`movie_fetch.py` and `movie_info.py` contain the classes that handle the ingestion. The `main.py` script is the primary driver for this task.
