from datetime import date, datetime
from pyspark.sql import Row

from beyond_bets.transforms import market_hourly, player_daily, player_hourly


def test_market_hourly_transform(spark):
    """Test the MarketHourly transform class"""
    rows = [
        Row(market="MLB", timestamp="2025-05-18T10:15:00", bet_amount=40),
        Row(market="MLB", timestamp="2025-05-18T10:45:00", bet_amount=60),
        Row(market="NFL", timestamp="2025-05-18T11:05:00", bet_amount=30),
    ]
    df = spark.createDataFrame(rows)
    transform = market_hourly.MarketHourly()
    transform.bets = df
    result = transform._transformation()
    output = {tuple(r.asDict().values()) for r in result.collect()}
    expected = {
        ("MLB", datetime(2025, 5, 18, 10, 0, 0), 100),
        ("NFL", datetime(2025, 5, 18, 11, 0, 0), 30),
    }
    assert output == expected


def test_player_daily_transform(spark):
    """Test the PlayerDaily transform class"""
    rows = [
        Row(player_id=1, timestamp="2025-05-18T14:23:00", bet_amount=50),
        Row(player_id=1, timestamp="2025-05-18T19:00:00", bet_amount=20),
        Row(player_id=2, timestamp="2025-05-19T01:00:00", bet_amount=30),
    ]
    df = spark.createDataFrame(rows)
    transform = player_daily.PlayerDaily()
    transform.bets = df
    result = transform._transformation()
    output = {tuple(r.asDict().values()) for r in result.collect()}
    expected = {
        (1, date(2025, 5, 18), 70),
        (2, date(2025, 5, 19), 30),
    }
    assert output == expected


def test_player_hourly_transform(spark):
    """Test the PlayerHourly transform class"""
    rows = [
        Row(player_id=1, timestamp="2025-05-18T14:23:00", bet_amount=50),
        Row(player_id=1, timestamp="2025-05-18T14:45:00", bet_amount=20),
        Row(player_id=2, timestamp="2025-05-18T15:05:00", bet_amount=30),
    ]
    df = spark.createDataFrame(rows)
    transform = player_hourly.PlayerHourly()
    transform.bets = df
    result = transform._transformation()
    output = {tuple(r.asDict().values()) for r in result.collect()}
    expected = {
        (1, datetime(2025, 5, 18, 14, 0, 0), 70),
        (2, datetime(2025, 5, 18, 15, 0, 0), 30),
    }
    assert output == expected
