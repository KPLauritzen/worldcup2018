# 2018 FIFA World Cup
Very much WORK IN PROGRESS!
Predicting the scores of all matches in the world cup. 

The intial idea is to use just ratings from FIFA 18.

Additionally, I wanted to learn [Luigi](https://luigi.readthedocs.io/), so that is how the data flow will be implemented

## Requirements
- Python 3.6
- BeautifulSoup4
- luigi
- pandas

## Usage

In `src/`, run
```
python luigitasks.py ProcessLeagueRatings --local-scheduler
```
to start the Luigi job. 
