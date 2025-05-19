from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from beyond_bets.utils.spark_session import get_spark
from pyspark.sql import functions as F


spark = get_spark()


class MarketDaily(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "MarketDaily"

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        return (
            self.bets.withColumn("date", F.to_date(F.col("timestamp")))
            .groupBy("market", "date")
            .agg(F.sum(F.col("bet_amount")).alias("total_bets"))
        )
