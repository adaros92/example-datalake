from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from beyond_bets.utils.spark_session import get_spark
from pyspark.sql import functions as F


spark = get_spark()


class MarketHourly(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "MarketHourly"

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        return (
            self.bets.withColumn("hour", F.date_trunc("hour", F.col("timestamp")))
            .groupBy("market", "hour")
            .agg(F.sum(F.col("bet_amount")).alias("total_bets"))
        )
