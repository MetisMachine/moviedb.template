import os
import sys
import json
import gzip
import requests
from time import sleep
from datetime import datetime, timedelta
from pytz import timezone
from logger import get_logger
from constants import *
from skafossdk import *

# Set up logger and initialize the skafos sdk
log = get_logger('oddcarl')
ska = Skafos()

# movie data class
class MovieData(object):

  def __init__(self, api_key, retry=3, backfilled_days=None, file_date=None, batch_size=50):
    self.api_key = api_key
    self.retry = retry
    self.base_url = "https://api.themoviedb.org/3/movie/"
    self.tz = timezone('EST')
    self.today = datetime.now(self.tz)
    self.batch_size = batch_size
    if not backfilled_days:
      if not file_date:
        sys.exit("You must supply either backfilled_days or a file_date")
      else:
        fdate = datetime.strptime(file_date, "%Y-%m-%d")
        self.filenames = [self._create_filename(fdate.day, fdate.month, fdate.year)]
    elif not isinstance(int(backfilled_days), int):
      sys.exit("Backfilled days must be an integer >= 0")
    elif int(backfilled_days) < 0:
      sys.exit("Backfilled days must be >= 0")
    elif int(backfilled_days) == 0:
      # Create the url to make the request
      self.filenames = [self._create_filename(self.today.day, self.today.month, self.today.year)]
      print(self.filenames)
    else:
      self.backfilled_days = int(backfilled_days)
      self.filenames = self._create_filenames()

  def _create_filenames(self):
    for day in range(self.backfilled_days+1):
      prior_date = self.today - timedelta(days=day)
      yield self._create_filename(prior_date.day, prior_date.month, prior_date.year)

  def _create_filename(self, day, month, year):
    """Filename constructor."""
    day = str(day)
    month = str(month)
    year = str(year)

    if len(day) == 1:
      day = '0' + day
    if len(month) == 1:
      month = '0' + month
    date_obj = month + '_' + day + '_' + year
    filename = 'movie_ids_' + date_obj + '.json.gz'
    return filename

  def _make_movie_file_request(self, filename):
    """"""
    retries = 0
    while retries <= self.retry:
      try:
        url = "http://files.tmdb.org/p/exports/{}".format(filename)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, "wb") as f:
          for chunk in response.iter_content(chunk_size=128):
            if chunk:  # filter out keep-alive new chunks
              f.write(chunk)
        break
      except requests.exceptions.HTTPError as e:
        log.debug("{}".format(e))
        break
      except Exception as e:
        log.debug("{}: Failed to make TMDB request on try {}".format(e, retries))
        retries += 1
        if retries <= self.retry:
          log.info("Trying again!")
          continue
        else:
          sys.exit("Max retries reached")

  def _open_movie_file(self, filename):
    """Open the downloaded gzip file and map rows to a curated list."""
    if not os.path.isfile(filename):
      self.movies = []
    else:
      with gzip.open(filename, 'rb') as f:
        d = f.readlines()
        self.movies = [parse_movie_file(line, filename) for line in d]
        log.info('Data found for {} movies!'.format(len(self.movies)))

  def _filter_popularity(self, pop):
    """Filter down movie list by popularity score."""
    self.movies = list(filter(lambda x: x['popularity'] >= pop, self.movies))

  def _fetch_imdb_id(self, movie_id):
    """GET request on movie database api to grab the external ids."""
    sleep(0.25)
    imdb_id_url = self.base_url + movie_id \
        + "/external_ids?api_key=" + self.api_key
    try:
      response = requests.get(imdb_id_url)
    except Exception as e:
      log.debug("{}: Failed to get imdb id for {}".format(e, movie_id))
      return None
    raw = json.loads(response.content)
    return raw.get('imdb_id', None)

  def _remove_file(self, filename):
    try:
      os.remove(filename)
    except OSError:
      log.debug("Unable to remove file {}".format(filename))

  def fetch(self, skafos, filter_pop=None):
    """Fetch the daily movie export list from movie database, parse the data, filter on popularity,
       and get the external imdb id."""
    log.info('Making request to TMDB for daily movie list export')
    for f in self.filenames:
      log.info('Retrieving movie file {}'.format(f))
      self._make_movie_file_request(f)
      self._open_movie_file(f)
      # If a filter value is provided - use it
      if filter_pop:
        self._filter_popularity(filter_pop)

      # Write the data
      self._write_data(skafos)

      # Remove the file
      self._remove_file(f)

    return self

  def _write_batches(self, engine, logger, schema, data):
    """Write batches of data to data engine."""
    for rows in batches(data, self.batch_size):
      res = engine.save(schema, list(rows)).result()
      logger.debug(res)

  def _write_data(self, skafos):
    """Write data out to the data engine in batches and filter out records where imdb_id is null
      as these will likely be unwanted films."""
    # Save out using the data engine
    movie_count = len(self.movies)
    log.info('Saving {} movie records with the data engine'.format(movie_count))
    if movie_count == 0:
      pass
    else:
      self._write_batches(skafos.engine, log, MOVIE_SCHEMA, self.movies)

def parse_movie_file(x, filename):
  """Parse the gzip file from the movie database request."""
  data = json.loads(x)
  file_date = date_from_filename(filename)
  return {'movie_id': str(data['id']),
          'movie_title': data['original_title'].strip(),
          'popularity': data['popularity'],
          'ingest_date': str(file_date[2]) + "-" + str(file_date[0]) + "-" + str(file_date[1]),
          'adult': data.get('adult'),
          'video': data.get('video')}

def date_from_filename(filename):
  """Extract the date from a filename."""
  return filename.split("movie_ids_")[1].split(".json.gz")[0].split("_")


def batches(iterable, n):
  """Divide a single list into a list of lists of size n"""
  batchLen = len(iterable)
  for ndx in range(0, batchLen, n):
    yield list(iterable[ndx:min(ndx + n, batchLen)])


if __name__ == "__main__":
  # Grab some environment variables using os module
  if 'MOVIE_DB' in os.environ:
    api_key = os.environ['MOVIE_DB']
  else:
    sys.exit('Please save a movie database api key in your environment.')

  if 'POPULARITY' in os.environ:
    pop = int(os.environ['POPULARITY'])
  else:
    pop = 15

  if 'BATCH_SIZE' in os.environ:
    n = int(os.environ['BATCH_SIZE'])
  else:
    n = 50

  if 'BACKFILLED_DAYS' in os.environ:
    bd = os.environ['BACKFILLED_DAYS']
  else:
    bd = None

  if 'FILE_DATE' in os.environ:
    fd = os.environ['FILE_DATE']
  else:
    fd = None

  # Fetch movie data and write to cassandra using the skafos data engine
  print("Backfill: %s" % bd, flush=True)
  daily_movie_update = MovieData(api_key, batch_size=n, backfilled_days=bd, file_date=fd).fetch(skafos=ska, filter_pop=pop)

