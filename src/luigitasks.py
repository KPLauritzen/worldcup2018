import luigi
import league
import international
import predictions

class MakeDatasets(luigi.WrapperTask):
    """Dummy task to trigger creating of both datasets."""
    league = luigi.Parameter(default='E0')
    match_day_start = luigi.IntParameter(default=174)
    match_day_end = luigi.IntParameter(default=242)
    season = luigi.Parameter('1718')
    def requires(self):
        yield league.MergeRangeMatchDaysLeague(league=self.league,
                                               match_day_start=self.match_day_start,
                                               match_day_end=self.match_day_end,
                                               season=self.season)
        yield international.MergeInternationalRatingsFixtures()
class DoPredictions(luigi.WrapperTask):
    """Dummy task to trigger final predictions"""

    def requires(self):
        yield predictions.TrainAndPredict()
    
if __name__ == '__main__':
    luigi.run()
