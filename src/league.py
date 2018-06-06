import luigi
from pathlib import Path
from bs4 import BeautifulSoup as BS
import pandas as pd
import numpy as np
import utils


data_dir = Path('../data/').resolve()
raw_data = data_dir / 'raw'
processed_data = data_dir / 'processed'
intermediate_data = data_dir / 'intermediate'

class DownloadLeagueFixtures(luigi.Task):
    """Download csv with team names and match results"""
    league = luigi.Parameter(default='E0')
    season = luigi.Parameter(default='1718')

    def output(self):
        path = str(raw_data / f'{self.season}/{self.league}_fixtures.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        url = f'http://www.football-data.co.uk/mmz4281/{self.season}/{self.league}.csv'
        page = utils.download_url(url)
        self.output().makedirs()
        filename = self.output().path
        with open(filename, 'wb') as outfile:
            outfile.write(page)

class DownloadLeagueRatings(luigi.Task):
    """Download fifa ratings for a specific league at a specific match day"""
    league = luigi.Parameter(default='E0')
    match_day = luigi.IntParameter(default=200)
    season = luigi.Parameter(default='1718')


    def output(self):
        path = str(raw_data / f'{self.season}/{self.match_day}/{self.league}.html')
        return luigi.LocalTarget(path=path)

    def run(self):
        fifa_season = utils.translate_season_to_fifa(self.season)
        league_int = utils.translate_league(self.league)
        url = f'https://www.fifaindex.com/teams/{fifa_season}_{self.match_day}/?league={league_int}'
        outpath = self.output()
        page = utils.download_url(url)
        outpath.makedirs()
        with open(outpath.path, 'wb') as f:
            f.write(page)

class ProcessLeagueFixtures(luigi.Task):
    """Process league fixtures, turning decimal odds into implied probability"""
    league = luigi.Parameter(default='E0')
    season = luigi.Parameter(default='1718')

    def requires(self):
        return DownloadLeagueFixtures(self.league, self.season)

    def output(self):
        path = str(intermediate_data / f'{self.season}/{self.league}_processed_fixtures.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        self.output().makedirs()
        df = pd.read_csv(self.input().path)
        df['odds_impl_prob_home'] = df.B365H.apply(utils.convert_decimal_odds_to_prob)
        df['odds_impl_prob_draw'] = df.B365D.apply(utils.convert_decimal_odds_to_prob)
        df['odds_impl_prob_away'] = df.B365A.apply(utils.convert_decimal_odds_to_prob)
        df.to_csv(self.output().path, index=False)

class ProcessLeagueRatings(luigi.Task):
    """Process ratings website to scrape team names and ratings"""
    league = luigi.Parameter(default='E0')
    match_day = luigi.IntParameter(default=200)
    season = luigi.Parameter(default='1718')
    def requires(self):
        return DownloadLeagueRatings(self.league, self.match_day, self.season)

    def output(self):
        path = str(intermediate_data / f'{self.season}/{self.match_day}/{self.league}_ratings.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        with open(self.input().path, 'rb') as f:
            page = f.read()
        soup = BS(page, 'html.parser')
        df = utils.soup_to_df(soup)
        df.Team = df.Team.apply(utils.translate_team_name)
        self.output().makedirs()
        df.to_csv(self.output().path, index=False)

class MergeLeagueFixturesRatings(luigi.Task):
    """Merge ratings and fixtures together on match day and team names"""
    league = luigi.Parameter(default='E0')
    match_day = luigi.IntParameter(default=200)
    season = luigi.Parameter('1718')

    def requires(self):
        return [
            ProcessLeagueRatings(self.league, self.match_day, self.season),
            ProcessLeagueFixtures(self.league, self.season)
        ]

    def output(self):
        path = str(intermediate_data / f'{self.season}/{self.match_day}/{self.league}_ratings_fixtures.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        """
        This merge is based on
        https://stackoverflow.com/questions/31328014/merging-dataframes-based-on-date-range
        """
        ratings_file, fixtures_file = self.input()
        df_ratings = pd.read_csv(ratings_file.path)
        df_fixtures = pd.read_csv(fixtures_file.path)
        df_fixtures.rename(mapper={'HomeTeam': 'team_home',
                  'AwayTeam': 'team_away',
                  'FTHG': 'goals_home',
                  'FTAG': 'goals_away'},
          axis=1, inplace=True)

        # Set dates
        df_ratings.Date = pd.to_datetime(df_ratings.Date)
        df_ratings.Date2 = df_ratings.Date + pd.Timedelta(7, unit='D')
        df_fixtures.Date = pd.to_datetime(df_fixtures.Date)
        df_ratings['merge_index'] = df_ratings.index - 10000

        # HOME TEAM
        df_fixtures['index_matched'] = np.piecewise(
            np.zeros(len(df_fixtures)),
            [
                (df_fixtures.Date.values >= start_date) &
                (df_fixtures.Date.values <= end_date) &
                (df_fixtures.team_home.values == team)
                for start_date, end_date, team in
                zip(df_ratings.Date.values, df_ratings.Date2.values, df_ratings.Team.values)
             ],
            df_ratings.merge_index.values
        )

        df_fixtures = df_fixtures.merge(df_ratings, how='inner', left_on='index_matched', right_on='merge_index')
        df_fixtures.drop(columns=['index_matched', 'merge_index', 'Team', 'Date_y'], inplace=True)
        translate_dict = {x:x+'_home' for x in ['ATT', 'DEF', 'MID', 'OVR']}
        translate_dict.update({'Date_x':'Date'})
        df_fixtures.rename(columns=translate_dict, inplace=True)


        # AWAY TEAM
        df_fixtures['index_matched'] = np.piecewise(
            np.zeros(len(df_fixtures)),
            [
                (df_fixtures.Date.values >= start_date) &
                (df_fixtures.Date.values <= end_date) &
                (df_fixtures.team_away.values == team)
                for start_date, end_date, team in
                zip(df_ratings.Date.values, df_ratings.Date2.values, df_ratings.Team.values)
             ],
            df_ratings.merge_index.values
        )

        df_fixtures = df_fixtures.merge(df_ratings, how='inner', left_on='index_matched', right_on='merge_index')
        df_fixtures.drop(columns=['index_matched', 'merge_index', 'Team', 'Date_y'], inplace=True)
        translate_dict = {x:x+'_away' for x in ['ATT', 'DEF', 'MID', 'OVR']}
        translate_dict.update({'Date_x':'Date'})
        df_fixtures.rename(columns=translate_dict, inplace=True)

        df_fixtures.to_csv(self.output().path, index=False)


class ReverseHomeAwayLeague(luigi.Task):
    """Switch home and away team to correct for home team advantage"""
    league = luigi.Parameter(default='E0')
    match_day = luigi.IntParameter(default=200)
    season = luigi.Parameter('1718')

    def requires(self):
        return MergeLeagueFixturesRatings(self.league, self.match_day, self.season)

    def output(self):
        path = str(intermediate_data / f'{self.season}/{self.match_day}/{self.league}_ratings_fixtures_reversed.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        self.output().makedirs()
        df_orig = pd.read_csv(self.input().path)
        df_rev = df_orig.copy()
        cols = list(df_orig)
        translate = {x:x[:-4] + 'away' for x in cols if x.endswith('home')}
        translate.update({x:x[:-4] + 'home' for x in cols if x.endswith('away')} )
        df_rev.rename(translate, axis=1, inplace=True)
        #concat = pd.concat([df_orig, df_rev], sort=True, axis=0)
        df_rev.to_csv(self.output().path, index=False)

class MergeRangeMatchDaysLeague(luigi.Task):
    """Concatenate a range of match days for a league. Default is all of 17/18
    season. This is the training dataset."""
    league = luigi.Parameter(default='E0')
    match_day_start = luigi.IntParameter(default=174)
    match_day_end = luigi.IntParameter(default=242)
    season = luigi.Parameter('1718')

    def requires(self):
        origs = [MergeLeagueFixturesRatings(self.league, md, self.season) for md in range(self.match_day_start, self.match_day_end)]
        reverse = [ReverseHomeAwayLeague(self.league, md, self.season) for md in range(self.match_day_start, self.match_day_end)]
        return origs + reverse

    def output(self):
        path = str(processed_data / f'{self.season}/{self.league}_collected.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        self.output().makedirs()
        collected_dfs = []
        for infile in self.input():
            df_in = pd.read_csv(infile.path)
            collected_dfs.append(df_in)
        concat_df = pd.concat(collected_dfs, sort=True, axis=0)
        concat_df.to_csv(self.output().path, index=False)
