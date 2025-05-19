from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()


class PlayerHourly(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "PlayerHourly"

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        return (
            self.bets.withColumn("hour", F.date_trunc("hour", F.col("timestamp")))
            .groupBy("player_id", "hour")
            .agg(F.sum(F.col("bet_amount")).alias("total_bets"))
        )
