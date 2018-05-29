import luigi
import pandas
from pathlib import Path
import urllib
from bs4 import BeautifulSoup as BS
import pandas as pd

data_dir = Path('../data/').resolve()
raw_data = data_dir / 'raw'
processed_data = data_dir / 'processed'
intermediate_data = data_dir / 'intermediate'

def soup_to_df(soup):
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
    return df

def make_list(elements):
    return [el.get_text() for el in elements]


def translate_league(league):
    trans_dict = {
        'E0' : '13'
    }
    return trans_dict[league]

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
    match_day = luigi.Parameter(default='200')

    def output(self):
        path = str(raw_data / f'{self.match_day}/{self.league}.html')
        return luigi.LocalTarget(path=path)

    def run(self):
        league_int = translate_league(self.league)
        url = f'https://www.fifaindex.com/teams/fifa18_{self.match_day}/?league={league_int}'
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
            with open(infile.path, 'r') as f:
                page = f.read()
                soup = BS(page, 'html.parser')
                df = soup_to_df(soup)
                dfs.append(df)
        df_concat = pd.concat(dfs, axis=0, ignore_index=True)
        self.output().makedirs()
        df_concat.to_csv(self.output().path, index=False)

class ProcessLeagueRatings(luigi.Task):
    league = luigi.Parameter(default='E0')
    match_day = luigi.Parameter(default='200')
    def requires(self):
        return DownloadLeagueRatings(self.league, self.match_day)

    def output(self):
        path = str(intermediate_data / f'{self.match_day}/{self.league}_ratings.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        with open(self.input().path, 'r') as f:
            page = f.read()
        soup = BS(page, 'html.parser')
        df = soup_to_df(soup)
        self.output().makedirs()
        df.to_csv(self.output().path, index=False)

if __name__ == '__main__':
    luigi.run()
