from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from beyond_bets.utils.spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql import Window


spark = get_spark()


class BetGrader(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "BetGrader"

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        # Default to previous 15 minutes if not specified
        beginning_of_window = int(kwargs.get("beginning_of_window", -15 * 60 * 1000))
        end_of_window = int(kwargs.get("end_of_window", 0))
        assert (
            beginning_of_window < end_of_window
        ), "beginning_of_window must be less than end_of_window"
        enriched_df = self.bets.withColumn(
            "timestamp_ms", F.col("timestamp").cast("timestamp").cast("long") * 1000
        )
        # Define lookback window for each market to calculate average bet amount
        window_spec = (
            Window.partitionBy("market")
            .orderBy("timestamp_ms")
            .rangeBetween(beginning_of_window, end_of_window)
        )
        # Calculate average bet amount over the lookback window
        graded_df = (
            enriched_df.withColumn(
                "avg_bet_lookback_window", F.avg("bet_amount").over(window_spec)
            )
            .withColumn("grade", F.col("bet_amount") / F.col("avg_bet_lookback_window"))
            .drop("timestamp_ms", "avg_bet_last_15min")
        )
        return graded_df
