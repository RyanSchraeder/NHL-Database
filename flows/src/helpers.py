import os 

def setup(
    source: str,
    endpoint: str = "https://www.hockey-reference.com/leagues/",
    year: int = dt.datetime.now().year
):
  """ Setup function that builds the URL for request at hockeyreference.com. Accepts the endpoint parameter and source to generate the appropriate URL.
      :param: source -> type of data to extract. 'seasons', 'playoffs', 'teams' stored in the paths variable within the function.
      :param: endpoint -> base url that is passed to the pipeline via the endpoint parameter. Default: https://www.hockey-reference.com/leagues/
      :year: year -> year of which to process data from. Default: current date at runtime
  """
    print(f'Received endpoint {endpoint} for source {source} and year {year}.')
    
    # Build endpoint URL & Filenames
    paths = ('seasons', 'playoffs', 'teams')
    if source in paths:
        if source == 'seasons':
            url = f"{endpoint}NHL_{year}_games.html#games"
            filename = f"NHL_{year}_regular_season"
        elif source == 'playoffs':
            url = f"{endpoint}NHL_{year}_games.html#games_playoffs"
            filename = f"NHL_{year}_playoff_season"
        elif source == 'teams':
            url = f"{endpoint}NHL_{year}.html#stats"
            filename = f"NHL_{year}_team_stats"
        else:
            pass
    else:
        print(f'Invalid source specified: {source}')
        sys.exit(1)

    print(f"URL Built: {url}\nDestination Filename: {filename}")
    return url, filename
