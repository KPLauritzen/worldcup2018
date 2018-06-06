import luigi
import league
import international

class MakeDatasets(luigi.WrapperTask):
    league = luigi.Parameter(default='E0')
    match_day_start = luigi.IntParameter(default=174)
    match_day_end = luigi.IntParameter(default=242)
    season = luigi.Parameter('1718')
    """Dummy task to trigger creating of both datasets."""
    def requires(self):
        yield league.MergeRangeMatchDaysLeague(league=self.league,
                                               match_day_start=self.match_day_start,
                                               match_day_end=self.match_day_end,
                                               season=self.season)
        yield international.MergeInternationalRatingsFixtures()
if __name__ == '__main__':
    luigi.run()
