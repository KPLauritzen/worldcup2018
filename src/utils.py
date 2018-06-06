import dateparser
import pandas as pd
import numpy as np
import urllib
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
def normalize(l):
    total = sum(l)
    return [el/total for el in l]

def convert_frac_odds_to_prob(odds_str):
    num, denom = odds_str.split('/')
    return int(denom) / (int(denom) + int(num))

def convert_decimal_odds_to_prob(odds):
    return 1/float(odds)

def get_odds(soup):
    home_teams, away_teams = [], []
    home_odds, draw_odds, away_odds = [], [], []
    matches = soup.find(id='fixtures').div.table.tbody.find_all('tr', attrs={'class':'match-on'})
    for match in matches:
        home_team, away_team = [x.contents[0] for x in match.find_all('p', attrs={'class':'fixtures-bet-name'})]
        home_win, draw, away_win = normalize([convert_frac_odds_to_prob(x.contents[0]) for x in match.find_all('span', attrs={'class':'odds'})])
        home_teams.append(home_team)
        away_teams.append(away_team)
        home_odds.append(home_win)
        draw_odds.append(draw)
        away_odds.append(away_win)
    df = pd.DataFrame()
    df['Home Team'] = home_teams
    df['Away Team'] = away_teams
    df['odds_impl_prob_home'] = home_odds
    df['odds_impl_prob_draw'] = draw_odds
    df['odds_impl_prob_away'] = away_odds
    return df

def translate_league(league):
    trans_dict = {
        'E0' : '13',  # Premier League
        'SP1' : '53', # La Liga
        'D1' : '19',  # Bundesliga
    }
    return trans_dict[league]

def translate_team_name(name):
    translate_dict = {
        #E0
        'Leicester City': 'Leicester',
        'Manchester City': 'Man City',
        'Manchester Utd': 'Man United',
        'Newcastle Utd': 'Newcastle',
        'Stoke City':'Stoke',
        'Spurs':'Tottenham',
        'Swansea City':'Swansea',
        #SP1
        'Athletic Bilbao':'Ath Bilbao',
        'Atlético Madrid':'Ath Madrid',
        'CD Leganés':'Leganes',
        'Celta Vigo':'Celta',
        'Deport. Alavés':'Alaves',
        'FC Barcelona':'Barcelona',
        'Getafe CF':'Getafe',
        'Girona CF':'Girona',
        'Levante UD':'Levante',
        'Málaga CF':'Málaga CF',
        'RC Deportivo':'La Coruna',
        'RCD Espanyol':'Espanol',
        'Real Betis':'Betis',
        'Real Sociedad':'Sociedad',
        'SD Eibar':'Eibar',
        'Sevilla FC':'Sevilla',
        'UD Las Palmas':'Las Palmas',
        'Valencia CF':'Valencia',
        'Villarreal CF':'Villarreal',
        #International
        'Korea Republic': 'South Korea'
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
