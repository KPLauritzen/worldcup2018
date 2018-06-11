import luigi
import international
import league

import pandas as pd
from pathlib import Path
from sklearn.linear_model import Ridge
import numpy as np

model_dir = Path('../models').resolve()
data_dir = Path('../data/').resolve()
processed_data = data_dir / 'processed'

class CollectTrainingData(luigi.Task):
    def requires(self):
        return [  league.MergeRangeMatchDaysLeague(league='E0'),
                  league.MergeRangeMatchDaysLeague(league='SP1')
        ]

    def output(self):
        path = str(processed_data / 'training_data.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        self.output().makedirs()
        dfs = []
        for inputfile in self.input():
            df = pd.read_csv(inputfile.path)
            all_cols = list(df)
            out_cols = [col for col in all_cols if col.endswith(('home', 'away', 'draw'))]
            dfs.append(df[out_cols])
        df_out = pd.concat(dfs, axis=0, ignore_index=False)
        df_out.to_csv(self.output().path, index=False)


class CollectTestData(luigi.Task):
    def requires(self):
        return international.MergeInternationalRatingsFixtures()

    def output(self):
        path = str(processed_data / 'test_data.csv')
        return luigi.LocalTarget(path=path)

    def run(self):
        self.output().makedirs()
        df = pd.read_csv(self.input().path)
        all_cols = list(df)
        out_cols = [col for col in all_cols if col.endswith(('home', 'away', 'draw'))]
        df_out = df[out_cols]
        df_out.to_csv(self.output().path, index=False)


class TrainAndPredict(luigi.Task):
    def requires(self):
        return [CollectTrainingData(), CollectTestData()]

    def output(self):
        path_raw = str(model_dir / 'predictions_raw.csv')
        path_rounded = str(model_dir / 'predictions_rounded.csv')
        return [luigi.LocalTarget(path=path_raw), luigi.LocalTarget(path=path_rounded)]

    def run(self):
        self.output()[0].makedirs()
        training_file, test_file = self.input()
        df_training = pd.read_csv(training_file.path)
        df_test = pd.read_csv(test_file.path)

        all_cols = list(df_training)
        feature_cols = [col for col in all_cols if col.startswith(('DEF', 'MID', 'ATT', 'OVR', 'odds'))]
        target_cols = ['goals_home', 'goals_away']

        X_train = df_training[feature_cols]
        Y_train = df_training[target_cols]
        X_test = df_test[feature_cols]


        clf = Ridge()
        clf.fit(X_train, Y_train)
        preds = np.clip(clf.predict(X_test), 0, 4).astype(np.float16)
        rounded_preds = np.around(preds).astype(int)
        df_test['pred_goals_home'] = preds[:,0]
        df_test['pred_goals_away'] = preds[:,1]
        df_test['int_goals_home'] = rounded_preds[:,0]
        df_test['int_goals_away'] = rounded_preds[:,1]
        raw_cols = ['team_home', 'pred_goals_home', 'pred_goals_away', 'team_away' ]
        df_out = df_test[raw_cols]
        df_out.to_csv(self.output()[0].path, index=False)

        int_cols = ['team_home', 'int_goals_home', 'int_goals_away', 'team_away' ]
        df_out = df_test[int_cols]
        df_out.to_csv(self.output()[1].path, index=False)
