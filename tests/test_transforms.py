from datetime import date, datetime, timedelta
from pyspark.sql import Row

from beyond_bets.transforms import (
    bet_grader,
    market_daily,
    market_hourly,
    player_daily,
    player_hourly,
    player_market_daily,
    top_players,
)


def test_market_daily_transform(spark):
    """Test the MarketDaily transform class"""
    rows = [
        Row(market="MLB", timestamp="2025-05-18T10:15:00", bet_amount=40),
        Row(market="MLB", timestamp="2025-05-18T10:45:00", bet_amount=60),
        Row(market="NFL", timestamp="2025-05-18T11:05:00", bet_amount=30),
    ]
    df = spark.createDataFrame(rows)
    transform = market_daily.MarketDaily()
    transform.bets = df
    result = transform._transformation()
    output = {tuple(r.asDict().values()) for r in result.collect()}
    expected = {
        ("MLB", date(2025, 5, 18), 100),
        ("NFL", date(2025, 5, 18), 30),
    }
    assert output == expected


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


def test_player_market_daily_transform(spark):
    """Test the PlayerMarketDaily transform class"""
    rows = [
        Row(player_id=1, market="MLB", timestamp="2025-05-18T10:15:00", bet_amount=40),
        Row(player_id=1, market="MLB", timestamp="2025-05-18T21:00:00", bet_amount=60),
        Row(player_id=2, market="NFL", timestamp="2025-05-18T11:00:00", bet_amount=30),
        Row(player_id=1, market="NFL", timestamp="2025-05-19T00:15:00", bet_amount=20),
    ]
    df = spark.createDataFrame(rows)
    transform = player_market_daily.PlayerMarketDaily()
    transform.bets = df
    result = transform._transformation()
    output = {tuple(r.asDict().values()) for r in result.collect()}
    expected = {
        (1, "MLB", date(2025, 5, 18), 100),
        (2, "NFL", date(2025, 5, 18), 30),
        (1, "NFL", date(2025, 5, 19), 20),
    }
    assert output == expected


def test_top_players_transform(spark):
    """Test the TopPlayers transform class"""
    now = datetime(2025, 5, 18, 12, 0, 0)
    week_ago = now - timedelta(days=7)
    rows = [
        Row(player_id=1, timestamp=str(now - timedelta(days=1)), bet_amount=10),
        Row(player_id=2, timestamp=str(now - timedelta(days=1)), bet_amount=20),
        Row(player_id=3, timestamp=str(now - timedelta(days=1)), bet_amount=30),
        Row(player_id=4, timestamp=str(now - timedelta(days=1)), bet_amount=40),
        Row(player_id=5, timestamp=str(now - timedelta(days=1)), bet_amount=1000),
    ]
    df = spark.createDataFrame(rows)
    transform = top_players.TopPlayers()
    transform.bets = df
    result = transform._transformation(now=now)
    output = [row["player_id"] for row in result.collect()]
    assert output == [5]


def test_bet_grader(spark):
    """Test the BetGrader transform class"""
    rows = [
        Row(market="A", timestamp="2025-05-18T09:30:00", bet_amount=17),
        Row(market="B", timestamp="2025-05-18T09:45:00", bet_amount=10),
        Row(market="B", timestamp="2025-05-18T10:00:00", bet_amount=5),
        Row(market="A", timestamp="2025-05-18T13:00:00", bet_amount=10),
        Row(market="A", timestamp="2025-05-18T13:05:00", bet_amount=20),
        Row(market="A", timestamp="2025-05-18T13:10:00", bet_amount=30),
        Row(market="A", timestamp="2025-05-18T13:15:00", bet_amount=15),
    ]
    df = spark.createDataFrame(rows)
    transform = bet_grader.BetGrader()
    transform.bets = df
    result = transform._transformation().select("timestamp", "bet_amount", "grade")
    result_data = {row["timestamp"]: round(row["grade"], 2) for row in result.collect()}
    assert result_data == {
        "2025-05-18T09:30:00": 1.0,
        "2025-05-18T09:45:00": 1.0,
        "2025-05-18T10:00:00": 0.67,
        "2025-05-18T13:00:00": 1.0,
        "2025-05-18T13:05:00": 1.33,
        "2025-05-18T13:10:00": 1.5,
        "2025-05-18T13:15:00": 0.8,
    }
