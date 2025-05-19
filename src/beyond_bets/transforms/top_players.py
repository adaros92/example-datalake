from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from beyond_bets.utils.spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql import Window


spark = get_spark()


class TopPlayers(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "TopPlayers"

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        # Filter bets from some trailing window
        now = kwargs.get("now") or datetime.utcnow()
        time_delta = kwargs.get("time_delta") or timedelta(days=7)
        assert time_delta.days > 0, "time_delta must be greater than 0"
        time_ago = now - timedelta(days=time_delta.days)
        recent_bets = self.bets.filter(
            F.col("timestamp") >= F.lit(time_ago.isoformat())
        )
        player_totals = recent_bets.groupBy("player_id").agg(
            F.sum("bet_amount").alias("total_bet_amount")
        )
        # Rank players by total spend
        window = Window.orderBy(F.desc("total_bet_amount"))
        ranked = player_totals.withColumn("percent_rank", F.percent_rank().over(window))
        # Keep only top 1%
        return ranked.filter(F.col("percent_rank") <= 0.01).drop("percent_rank")
