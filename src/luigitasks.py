import luigi
import pandas
from pathlib import Path
import urllib

data_dir = Path('../data/').resolve()
raw_data = data_dir / 'raw'
processed_data = data_dir / 'processed'

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

class DownloadInternationalRatingsToCsv(luigi.Task):
    pages = 2

    def output(self):
        paths = [str(raw_data / f'international_ratings_{ii}.csv')
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
        path = str(raw_data / f'{self.match_day}/{self.league}.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        league_int = translate_league(self.league)
        url = 'https://www.fifaindex.com/teams/fifa18_{self.match_day}/?league={league_int}'
        outpath = self.output()
        page = download_url(url)
        outpath.makedirs()
        with open(outpath.path, 'wb') as f:
            f.write(page)

if __name__ == '__main__':
    luigi.run()
