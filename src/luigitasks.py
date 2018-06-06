import luigi
import league
import international

class MakeDatasets(luigi.WrapperTask):
    """Dummy task to trigger creating of both datasets."""
    def requires(self):
        yield league.MergeRangeMatchDaysLeague()
        yield international.MergeInternationalRatingsFixtures()
if __name__ == '__main__':
    luigi.run()
