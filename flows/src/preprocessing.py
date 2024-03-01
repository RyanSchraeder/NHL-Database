# import the dataframes from the etl output to perform preprocessing and create a training and testing set
# The regular season data and team statistics should be merged here to create the final dataset.
# Accepts JSON data from S3 files and creates a final dataframe.

import pandas as pd
import warnings
from pydantic import BaseModel, ValidationError, ConfigDict
from typing import Optional, List
from datetime import datetime as dt

# Suppress FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)


class DataTransform(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataframe: pd.DataFrame
    date: dt

    @classmethod
    def seasons(cls, dataframe):

        dataframe = dataframe.rename(columns=({
            'Date': 'date', 'Visitor': 'away_team_id', 'Home': 'home_team_id', 'G': 'away_goals', 'G.1': 'home_goals',
            'LOG': 'length_of_game_min'
        }))
        dataframe = dataframe[
            ['date', 'away_team_id', 'away_goals', 'home_team_id', 'home_goals', 'length_of_game_min']
        ]

        dataframe['updated_at'] = dt.now()

        # Transforming data
        dataframe['length_of_game_min'] = dataframe['length_of_game_min'].apply(
            lambda x: (int(str(x).split(":")[0]) * 60) + (int(str(x).split(":")[1])) if str(x).lower() != 'nan' else x
        )

        dataframe.date = dataframe.date.apply(pd.to_datetime)

        return dataframe

    @classmethod
    def teams(cls, dataframe):

        print(f"Sample data for teams: {dataframe.head(1)}")
        
        # # Team name cleaning
        # dataframe['Team'] = [str(i).replace('*', '') for i in dataframe['Unnamed: 0']]
        
        # Creating Column for Total Goals 
        dataframe['G'] = dataframe.GF + dataframe.GA 
        
        # Creating Column for Total Power-Play Goals 
        dataframe['PPG'] = dataframe.PP + dataframe.PPA
        
        # Creating Column for Total Games in Shootouts
        dataframe['SHOOTOUTS'] = dataframe.SOW + dataframe.SOL

        # Converts percentages
        for column, row in dataframe.iteritems(): 
            if '%' in column:
                for item in row: 
                    if item < 1: 
                        row += row * 100

        dataframe['updated_at'] = dt.now()
        
        return dataframe

        
    @classmethod
    def add_fake_data(
        cls, dataframe, date, away_teams, home_teams,
        away_goals, home_goals, away_result, home_result,
        game_length
    ):
        """ Add fake data to simulate a matchup and predict its outcome. Only necessary if testing the model out.
        This same function was used to simulate the Stanley Cup matchup in 2022.
        """
        try:
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

            result = pd.concat([dataframe, fake_data], ignore_index=True)
            return result
        except Exception as e:
            if e == ValidationError:
                print(f'Validation of data failed. Context: {e}')

    @classmethod
    def encoding_full(
        cls, dataframe, date_col: str
    ):
        # Encode non-numeric variables
        dataframe.home_team = dataframe.home_team.astype('category').cat.codes
        dataframe.away_team = dataframe.away_team.astype('category').cat.codes

        # Encode date as a day of week to avoid time series implication.
        dataframe.rename(columns={f'{date_col}': 'day_of_week'}, inplace=True)
        dataframe['day_of_week'] = dataframe['day_of_week'].dt.dayofweek

        return dataframe
