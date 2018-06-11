# 2018 FIFA World Cup
Predicting the scores of all matches in the world cup.

The intial idea is to use just ratings from FIFA 18.

Additionally, I wanted to learn [Luigi](https://luigi.readthedocs.io/), so that is how the data flow will be implemented

## Requirements
- Python 3.6
- BeautifulSoup4
- luigi
- pandas
- numpy
- dateparser

## Usage

In `src/`, run
```
python luigitasks.py MakeDatasets --local-scheduler
```
or 
```
python luigitasks.py DoPredictions --local-scheduler
```
to start the Luigi job.
Training data in `data/processed/1718/E0_collected.csv` and prediction data in `data/processed/international_ratings_fixtures.csv`

## Current best predictions

Using Ridge regression and the following features:

```
['ATT_away',
 'ATT_home',
 'DEF_away',
 'DEF_home',
 'MID_away',
 'MID_home',
 'OVR_away',
 'OVR_home',
 'odds_impl_prob_away',
 'odds_impl_prob_draw',
 'odds_impl_prob_home']

```

I get these predictions:
```
| team_home    | int_goals_home | int_goals_away | team_away    |
|--------------+----------------+----------------+--------------|
| Russia       |              2 |              1 | Saudi Arabia |
| Egypt        |              0 |              2 | Uruguay      |
| Morocco      |              1 |              1 | Iran         |
| Portugal     |              1 |              2 | Spain        |
| France       |              2 |              1 | Australia    |
| Argentina    |              2 |              1 | Iceland      |
| Peru         |              1 |              1 | Denmark      |
| Croatia      |              2 |              1 | Nigeria      |
| Costa Rica   |              1 |              1 | Serbia       |
| Germany      |              2 |              1 | Mexico       |
| Brazil       |              2 |              1 | Switzerland  |
| Sweden       |              1 |              1 | South Korea  |
| Belgium      |              2 |              1 | Panama       |
| Tunisia      |              1 |              2 | England      |
| Colombia     |              2 |              1 | Japan        |
| Poland       |              1 |              1 | Senegal      |
| Russia       |              1 |              1 | Egypt        |
| Portugal     |              2 |              1 | Morocco      |
| Uruguay      |              2 |              0 | Saudi Arabia |
| Iran         |              1 |              2 | Spain        |
| Denmark      |              2 |              1 | Australia    |
| France       |              2 |              1 | Peru         |
| Argentina    |              2 |              1 | Croatia      |
| Brazil       |              2 |              0 | Costa Rica   |
| Nigeria      |              1 |              1 | Iceland      |
| Serbia       |              1 |              1 | Switzerland  |
| Belgium      |              2 |              1 | Tunisia      |
| South Korea  |              1 |              2 | Mexico       |
| Germany      |              2 |              1 | Sweden       |
| England      |              2 |              1 | Panama       |
| Japan        |              1 |              1 | Senegal      |
| Poland       |              1 |              1 | Colombia     |
| Uruguay      |              1 |              1 | Russia       |
| Saudi Arabia |              1 |              2 | Egypt        |
| Iran         |              1 |              2 | Portugal     |
| Spain        |              2 |              1 | Morocco      |
| Denmark      |              1 |              2 | France       |
| Australia    |              1 |              1 | Peru         |
| Nigeria      |              1 |              2 | Argentina    |
| Iceland      |              1 |              2 | Croatia      |
| Mexico       |              1 |              1 | Sweden       |
| South Korea  |              1 |              2 | Germany      |
| Serbia       |              1 |              2 | Brazil       |
| Switzerland  |              1 |              1 | Costa Rica   |
| Japan        |              1 |              1 | Poland       |
| Senegal      |              1 |              1 | Colombia     |
| Panama       |              1 |              1 | Tunisia      |
| England      |              1 |              1 | Belgium      |
```
