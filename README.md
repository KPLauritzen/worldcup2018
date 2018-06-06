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
- numpy
- dateparser

## Usage

In `src/`, run
```
python luigitasks.py MakeDatasets --local-scheduler
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
       Home Team  raw_pred_home     Away Team  raw_pred_away
0         Russia       2.042203  Saudi Arabia       0.518734
1          Egypt       0.706287       Uruguay       1.893057
2        Morocco       1.219219          Iran       0.861794
3       Portugal       1.247921         Spain       1.891909
4         France       2.244142     Australia       0.690781
5      Argentina       2.294835       Iceland       0.798458
6           Peru       1.069733       Denmark       1.239713
7        Croatia       1.593400       Nigeria       0.696920
8     Costa Rica       0.728601        Serbia       1.282627
9        Germany       2.053961        Mexico       0.931255
10        Brazil       2.116346   Switzerland       0.710945
11        Sweden       1.240764   South Korea       0.743119
12       Belgium       2.080713        Panama       0.518293
13       Tunisia       0.658195       England       2.023583
14      Colombia       1.662429         Japan       0.741643
15        Poland       1.376247       Senegal       0.947026
16        Russia       1.481164         Egypt       0.902973
17      Portugal       1.889775       Morocco       0.917230
18       Uruguay       2.324375  Saudi Arabia       0.495886
19          Iran       0.621456         Spain       2.209856
20       Denmark       1.640454     Australia       0.722825
21        France       1.972626          Peru       0.880635
22     Argentina       1.794317       Croatia       1.113064
23        Brazil       2.111859    Costa Rica       0.595838
24       Nigeria       1.125731       Iceland       1.123164
25        Serbia       1.095467   Switzerland       1.118110
26       Belgium       2.085627       Tunisia       0.675258
27   South Korea       0.863737        Mexico       1.471512
28       Germany       1.896799        Sweden       0.807207
29       England       2.061755        Panama       0.485179
30         Japan       0.935351       Senegal       1.202385
31        Poland       1.228969      Colombia       1.316351
32       Uruguay       1.674282        Russia       1.321042
33  Saudi Arabia       0.499946         Egypt       1.469491
34          Iran       0.748829      Portugal       2.124423
35         Spain       2.029489       Morocco       0.762290
36       Denmark       1.113405        France       1.927890
37     Australia       0.782830          Peru       1.256540
38       Nigeria       0.835634     Argentina       2.049941
39       Iceland       0.857232       Croatia       1.584249
40        Mexico       1.264011        Sweden       1.184238
41   South Korea       0.651692       Germany       1.979851
42        Serbia       0.733367        Brazil       2.088096
43   Switzerland       1.330452    Costa Rica       0.744667
44         Japan       0.755871        Poland       1.476555
45       Senegal       0.900877      Colombia       1.597882
46        Panama       0.752275       Tunisia       0.973760
47       England       1.502211       Belgium       1.527783

```
