import luigi
import pandas
from pathlib import Path
import urllib
from bs4 import BeautifulSoup as BS
import pandas as pd
from datetime import datetime
import numpy as np
import dateparser


data_dir = Path('../data/').resolve()
raw_data = data_dir / 'raw'
processed_data = data_dir / 'processed'
intermediate_data = data_dir / 'intermediate'

def soup_to_df(soup):
    date_string = soup.find_all('a', attrs={'class': 'dropdown-toggle'})[-1].contents[0]
    date = dateparser.parse(date_string)
    team_els = soup.tbody.find_all('td', attrs={'data-title':'Name'})
    team_list = make_list(team_els)
    att_els = soup.tbody.find_all('td', attrs={'data-title':'ATT'})
    att_list = make_list(att_els)
    mid_els = soup.tbody.find_all('td', attrs={'data-title':'MID'})
    mid_list = make_list(mid_els)
    def_els = soup.tbody.find_all('td', attrs={'data-title':'DEF'})
    def_list = make_list(def_els)
    ovr_els = soup.tbody.find_all('td', attrs={'data-title':'OVR'})
    ovr_list = make_list(ovr_els)
    df = pd.DataFrame()
    df['Team'] = team_list
    df['ATT'] = att_list
    df['MID'] = mid_list
    df['DEF'] = def_list
    df['OVR'] = ovr_list
    df['Date'] = date.strftime('%Y-%m-%d')
    return df

def make_list(elements):
    return [el.get_text() for el in elements]


def translate_league(league):
    trans_dict = {
        'E0' : '13'
    }
    return trans_dict[league]

def translate_team_name(name):
    translate_dict = {
        'Leicester City': 'Leicester',
        'Manchester City': 'Man City',
        'Manchester Utd': 'Man United',
        'Newcastle Utd': 'Newcastle',
        'Stoke City':'Stoke',
        'Spurs':'Tottenham'
    }
    if not name in translate_dict: return name
    else:
        return translate_dict[name]

def translate_season_to_fifa(season):
    return f'fifa{str(season)[-2:]}'

def download_url(url):
    request = urllib.request.Request(url, headers={'user-agent':'Mozilla/5.0'})
    conn = urllib.request.urlopen(request)
    page = conn.read()
    return page

class DownloadFixtures(luigi.Task):
    league = luigi.Parameter(default='E0')
    season = luigi.Parameter(default='1718')

    def output(self):
        path = str(raw_data / f'{self.season}/{self.league}.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        url = f'http://www.football-data.co.uk/mmz4281/{self.season}/{self.league}.csv'
        page = download_url(url)
        self.output().makedirs()
        filename = self.output().path
        with open(filename, 'wb') as outfile:
            outfile.write(page)

class DownloadInternationalRatings(luigi.Task):
    pages = 2

    def output(self):
        paths = [str(raw_data / f'international_ratings_{ii}.html')
                 for ii in range(1, self.pages+1)]
        return [luigi.LocalTarget(path=path) for path in paths]

    def run(self):
        base_url = 'https://www.fifaindex.com/teams/{}/?type=1'
        for ii, outpath in enumerate(self.output()):
            url = base_url.format(ii+1)
            page = download_url(url)
            outpath.makedirs()
            with open(outpath.path, 'wb') as f:
                f.write(page)

class DownloadLeagueRatings(luigi.Task):
    league = luigi.Parameter(default='E0')
    match_day = luigi.IntParameter(default=200)
    season = luigi.Parameter(default='1718')


    def output(self):
        path = str(raw_data / f'{self.season}/{self.match_day}/{self.league}.html')
        return luigi.LocalTarget(path=path)

    def run(self):
        fifa_season = translate_season_to_fifa(self.season)
        league_int = translate_league(self.league)
        url = f'https://www.fifaindex.com/teams/{fifa_season}_{self.match_day}/?league={league_int}'
        outpath = self.output()
        page = download_url(url)
        outpath.makedirs()
        with open(outpath.path, 'wb') as f:
            f.write(page)

class ProcessInternationalRatings(luigi.Task):
    def requires(self):
        return DownloadInternationalRatings()

    def output(self):
        path = str(intermediate_data / f'international_ratings.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        dfs = []
        for infile in self.input():
            with open(infile.path, 'rb') as f:
                page = f.read()
                soup = BS(page, 'html.parser')
                df = soup_to_df(soup)
                dfs.append(df)
        df_concat = pd.concat(dfs, axis=0, ignore_index=True)
        self.output().makedirs()
        df_concat.to_csv(self.output().path, index=False)

class ProcessLeagueRatings(luigi.Task):
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
        df = soup_to_df(soup)
        df.Team = df.Team.apply(translate_team_name)
        self.output().makedirs()
        df.to_csv(self.output().path, index=False)

class MergeLeagueFixturesRatings(luigi.Task):
    league = luigi.Parameter(default='E0')
    match_day = luigi.IntParameter(default=200)
    season = luigi.Parameter('1718')

    def requires(self):
        return [
            ProcessLeagueRatings(self.league, self.match_day, self.season),
            DownloadFixtures(self.league, self.season)
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

class DownloadInternationalFixtures(luigi.Task):
    def output(self):
        path = str(raw_data / 'international_fixtures.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        url = 'https://fixturedownload.com/download/fifa-world-cup-2018-RussianStandardTime.csv'
        page = download_url(url)
        self.output().makedirs()
        filename = self.output().path
        with open(filename, 'wb') as outfile:
            outfile.write(page)

class MergeInternationalRatingsFixtures(luigi.Task):
    def requires(self):
        return [
            JoinAdditionalInternationalRatings(),
            DownloadInternationalFixtures()
        ]

    def output(self):
        path = str(processed_data / 'international_ratings_fixtures.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        ratings_file, fixtures_file = self.input()
        df_ratings = pd.read_csv(ratings_file.path)
        df_fixtures = pd.read_csv(fixtures_file.path).loc[:47, :]

        # HOME
        df_fixtures = df_fixtures.merge(df_ratings, left_on='Home Team', right_on='Team', how='left')
        translate_dict = {x:x+'_home' for x in ['ATT', 'DEF', 'MID', 'OVR']}
        df_fixtures.rename(columns=translate_dict, inplace=True)
        # AWAY
        df_fixtures = df_fixtures.merge(df_ratings, left_on='Away Team', right_on='Team', how='left')
        translate_dict = {x:x+'_away' for x in ['ATT', 'DEF', 'MID', 'OVR']}
        df_fixtures.rename(columns=translate_dict, inplace=True)

        df_fixtures.to_csv(self.output().path, index=False)

class DownloadAdditionalInternationalRatings(luigi.Task):
    def output(self):
        paths = [str(raw_data / f'international_ratings_additional{ii}.html') for ii in range(1,9)]
        return [luigi.LocalTarget(path=path) for path in paths]

    def run(self):
        for ii, filename in enumerate(self.output()):
            filename.makedirs()
            url = f'https://www.futhead.com/18/nations/?page={ii+1}'
            page = download_url(url)
            with open(filename.path, 'wb') as outfile:
                outfile.write(page)

class ProcessAdditionalInternationalRatings(luigi.Task):
    def requires(self):
        return DownloadAdditionalInternationalRatings()

    def output(self):
        path = str(intermediate_data / 'international_ratings_additional.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        dfs = []
        for filename in self.input():
            with open(filename.path, 'rb') as f:
                page = f.read()
            soup = BS(page, 'html.parser')
            teams = []
            stats = []
            for line in soup.find_all('li',  attrs={'class':'list-group-item'}):
                name = line.find('span', attrs={'class':'player-name'})
                if name is None:
                    continue
                stat = int(line.find('span', attrs={'class':'value'}).contents[0])
                teams.append(name.contents[0])
                stats.append(stat)
            df = pd.DataFrame()
            df['Team'] = teams
            df['Backup Score'] = stats
            dfs.append(df)
        df_concat = pd.concat(dfs, axis=0, ignore_index=True)
        df_maxed = df_concat.groupby('Team').max().reset_index()
        df_maxed.to_csv(self.output().path, index=False)

class JoinAdditionalInternationalRatings(luigi.Task):
    def requires(self):
        return [
            ProcessInternationalRatings(),
            ProcessAdditionalInternationalRatings()
        ]

    def output(self):
        path = str(intermediate_data / 'international_ratings_joined.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        file_ratings, file_add = self.input()
        df_ratings = pd.read_csv(file_ratings.path)
        df_add = pd.read_csv(file_add.path)
        merged = df_ratings.merge(df_add, how='outer', on='Team')

        for val in ['ATT', 'DEF', 'MID', 'OVR']:
            merged[val] = merged[val].fillna(merged['Backup Score'], axis=0)

        merged.to_csv(self.output().path)
if __name__ == '__main__':
    luigi.run()
