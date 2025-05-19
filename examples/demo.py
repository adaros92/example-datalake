from pyspark.sql import functions as F
from beyond_bets.transforms.player_daily import PlayerDaily
from beyond_bets.transforms.player_hourly import PlayerHourly
from beyond_bets.transforms.market_daily import MarketDaily
from beyond_bets.transforms.market_hourly import MarketHourly
from beyond_bets.transforms.player_market_daily import PlayerMarketDaily
from beyond_bets.transforms.top_players import TopPlayers
from beyond_bets.transforms.bet_grader import BetGrader


pd = PlayerDaily()
pd.result().orderBy(F.col("total_bets").desc()).limit(10).show()

ph = PlayerHourly()
ph.result().orderBy(F.col("total_bets").desc()).limit(10).show()

md = MarketDaily()
md.result().orderBy(F.col("total_bets").desc()).limit(10).show()

mh = MarketHourly()
mh.result().orderBy(F.col("total_bets").desc()).limit(10).show()

pmd = PlayerMarketDaily()
pmd.result().orderBy(F.col("total_bets").desc()).limit(10).show()

tp = TopPlayers()
tp.result().orderBy(F.col("total_bet_amount").desc()).limit(10).show()

bg = BetGrader()
bg.result().orderBy(F.col("timestamp").desc()).limit(10).show()
