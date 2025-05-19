from beyond_bets.transforms import market_hourly, player_daily


def test_market_hourly_transform(spark):
    """Test the MarketHourly transform class"""
    market_hourly_transform = market_hourly.MarketHourly()
    assert market_hourly_transform.name == "PlayerDaily"
    # Expected schema
    result = market_hourly_transform.result()
    expected_columns = {"market", "hour", "total_bets"}
    assert all(col in result.columns for col in expected_columns)
    # Expected number of records
    assert result.count() == 961
    # Expected distinct markets
    assert result.select("market").distinct().count() == 4


def test_player_daily_transform(spark):
    """Test the PlayerDaily transform class"""
    player_daily_transform = player_daily.PlayerDaily()
    assert player_daily_transform.name == "PlayerDaily"
    # Expected schema
    result = player_daily_transform.result()
    result.show(truncate=False)
    expected_columns = {"player_id", "date", "total_bets"}
    assert all(col in result.columns for col in expected_columns)
    # Expected number of records
    assert result.count() == 99995
    # Expected distinct players
    assert result.select("player_id").distinct().count() == 10000