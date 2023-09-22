# import the dataframes from the etl output to perform preprocessing and create a training and testing set
# The regular season data and team statistics should be merged here to create the final dataset.
# Accepts JSON data from S3 files and creates a final dataframe.

import pandas as pd
from datetime import datetime as dt


def add_fake_data(
        data, date, away_teams, home_teams,
        away_goals, home_goals, away_result, home_result,
        game_length
):
    """ Add fake data to simulate a matchup and predict its outcome. Only necessary if testing the model out.
    This same function was used to simulate the Stanley Cup matchup in 2022.
    """
    fake_data = pd.DataFrame({
        'date': date,
        'away_team': away_teams,
        'home_team': home_teams,
        'away_goals': away_goals,
        'home_goals': home_goals,
        'away_outcome': away_result,
        'home_outcome': home_result,
        'length_of_game_min': game_length
    })

    result = pd.concat([data, fake_data], ignore_index=True)
    return result


dates = ['2022-06-15', '2022-06-18', '2022-06-20', '2022-06-22', '2022-06-24', '2022-06-26']
dates_list = [dt.strptime(date, '%Y-%m-%d') for date in dates]

away_teams = [
    'Tampa Bay Lightning',
    'Tampa Bay Lightning',
    'Colorado Avalanche',
    'Colorado Avalanche',
    'Tampa Bay Lightning',
    'Colorado Avalanche'
]
home_teams = [
    'Colorado Avalanche',
    'Colorado Avalanche',
    'Tampa Bay Lightning',
    'Tampa Bay Lightning',
    'Colorado Avalanche',
    'Tampa Bay Lightning'
]
stanley_cup_df = add_fake_data(
    data=games_df,
    date=dates_list,
    away_teams=away_teams,
    home_teams=home_teams,
    away_goals=[games_df.groupby('away_team')['away_goals'].mean().loc[i] for i in away_teams],
    home_goals=[games_df.groupby('home_team')['home_goals'].mean().loc[i] for i in home_teams],
    away_result=[games_df.groupby('away_team')['away_outcome'].mean().loc[i] for i in away_teams],
    home_result=[games_df.groupby('home_team')['home_outcome'].mean().loc[i] for i in home_teams],
    game_length=[games_df['length_of_game_min'].mean() for i in range(6)]
)
stanley_cup_df = encoding_game_outcome(stanley_cup_df, 'away_outcome', 'home_outcome', 'away_outcome', 'home_outcome')

# View preview
stanley_cup_df.tail(6)

def encoding_full(
        df, home_team: str, away_team: str, date_col: str
):
    # Encode non-numeric variables
    df.home_team = df.home_team.astype('category').cat.codes
    df.away_team = df.away_team.astype('category').cat.codes

    # Encode date as a day of week to avoid time series implication.
    df.rename(columns={f'{date_col}': 'day_of_week'}, inplace=True)
    df['day_of_week'] = df['day_of_week'].dt.dayofweek

    return df


classification_df = encoding_full(
    clean_cup_data, 'home_team', 'away_team', 'date'
)

classification_df = classification_df[[
    'day_of_week', 'away_team', 'away_goals', 'home_team', 'home_goals', 'length_of_game_min',
    'away_outcome', 'home_outcome', 'W', 'L', 'GDIFF', 'SOW%', 'PTS', 'PTS%', 'PP', 'PP%'
]]

classification_df = classification_df.iloc[:, 0:].astype(int)

# Output dataset with all encoded variables to training set. Will be used for training model.
classification_df.to_csv(os.path.join(path, "new_encoded_variables.csv"), index=False)
