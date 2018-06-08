import luigi
from pathlib import Path
from bs4 import BeautifulSoup as BS
import pandas as pd
import utils


data_dir = Path('../data/').resolve()
raw_data = data_dir / 'raw'
processed_data = data_dir / 'processed'
intermediate_data = data_dir / 'intermediate'

class DownloadInternationalRatings(luigi.Task):
    """Download most current FIFA 18 ratings for International teams"""
    pages = 2

    def output(self):
        paths = [str(raw_data / f'international_ratings_{ii}.html')
                 for ii in range(1, self.pages+1)]
        return [luigi.LocalTarget(path=path) for path in paths]

    def run(self):
        base_url = 'https://www.fifaindex.com/teams/{}/?type=1'
        for ii, outpath in enumerate(self.output()):
            url = base_url.format(ii+1)
            page = utils.download_url(url)
            outpath.makedirs()
            with open(outpath.path, 'wb') as f:
                f.write(page)


class ProcessInternationalRatings(luigi.Task):
    """Parse HTML to get team names and FIFA ratings"""
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
                df = utils.soup_to_df(soup)
                dfs.append(df)
        df_concat = pd.concat(dfs, axis=0, ignore_index=True)
        self.output().makedirs()
        df_concat.to_csv(self.output().path, index=False)


class DownloadInternationalFixtures(luigi.Task):
    """Download Team names for matches to be played at WC2018.
    Note, we are only using 48 first matches because the rest is Play-offs."""
    def output(self):
        path = str(raw_data / 'international_fixtures.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        url = 'https://fixturedownload.com/download/fifa-world-cup-2018-RussianStandardTime.csv'
        page = utils.download_url(url)
        self.output().makedirs()
        filename = self.output().path
        with open(filename, 'wb') as outfile:
            outfile.write(page)

class DownloadInternationalOdds(luigi.Task):
    """Download odds on home/away win and draw."""
    def output(self):
        path = str(raw_data / 'international_odds.html')
        return luigi.LocalTarget(path=path)

    def run(self):
        self.output().makedirs()
        url = 'https://www.oddschecker.com/football/world-cup#outrights'
        page = utils.download_url(url)
        self.output().makedirs()
        filename = self.output().path
        with open(filename, 'wb') as outfile:
            outfile.write(page)


class ProcessInternationalOdds(luigi.Task):
    """Process odds from fractional format to implied probability."""
    def requires(self):
        return DownloadInternationalOdds()

    def output(self):
        path = str(intermediate_data / 'international_odds.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        self.output().makedirs()
        with open(self.input().path, 'rb') as f:
            page = f.read()
        soup = BS(page, 'html.parser')
        df_odds = utils.get_odds(soup)
        df_odds.to_csv(self.output().path, index=False)

class DownloadAdditionalInternationalRatings(luigi.Task):
    """Download extra ratings for teams that are missing from the Fifa database"""
    def output(self):
        paths = [str(raw_data / f'international_ratings_additional{ii}.html') for ii in range(1,9)]
        return [luigi.LocalTarget(path=path) for path in paths]

    def run(self):
        for ii, filename in enumerate(self.output()):
            filename.makedirs()
            url = f'https://www.futhead.com/18/nations/?page={ii+1}'
            page = utils.download_url(url)
            with open(filename.path, 'wb') as outfile:
                outfile.write(page)

class ProcessAdditionalInternationalRatings(luigi.Task):
    """Parse website with extra team ratings"""
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
        self.output().makedirs()
        df_maxed.to_csv(self.output().path, index=False)

class JoinAdditionalInternationalRatings(luigi.Task):
    """Merge extra ratings onto the existing ratings"""
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

class MergeInternationalRatingsFixtures(luigi.Task):
    """Merge all the information into one table"""
    def requires(self):
        return [
            JoinAdditionalInternationalRatings(),
            DownloadInternationalFixtures(),
            ProcessInternationalOdds()
        ]

    def output(self):
        path = str(processed_data / 'international_ratings_fixtures.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        self.output().makedirs()
        ratings_file, fixtures_file, odds_file = self.input()
        df_ratings = pd.read_csv(ratings_file.path)
        df_fixtures = pd.read_csv(fixtures_file.path).loc[:47, :]
        df_odds = pd.read_csv(odds_file.path)

        # Fix team names
        df_ratings.Team = df_ratings.Team.apply(utils.translate_team_name)

        df_fixtures['team_home'] = df_fixtures['Home Team'].apply(utils.translate_team_name)
        df_fixtures['team_away'] = df_fixtures['Away Team'].apply(utils.translate_team_name)

        df_odds['team_home'] = df_odds['Home Team'].apply(utils.translate_team_name)
        df_odds['team_away'] = df_odds['Away Team'].apply(utils.translate_team_name)


        # HOME
        df_fixtures = df_fixtures.merge(df_ratings, left_on='team_home', right_on='Team', how='left')
        translate_dict = {x:x+'_home' for x in ['ATT', 'DEF', 'MID', 'OVR']}
        df_fixtures.rename(columns=translate_dict, inplace=True)
        # AWAY
        df_fixtures = df_fixtures.merge(df_ratings, left_on='team_away', right_on='Team', how='left')
        translate_dict = {x:x+'_away' for x in ['ATT', 'DEF', 'MID', 'OVR']}
        df_fixtures.rename(columns=translate_dict, inplace=True)

        df_fixtures = df_fixtures.merge(df_odds, on=['team_home', 'team_away'], how='left')
        df_fixtures.to_csv(self.output().path, index=False)
