from beyond_bets.datasets import bets, numbers


def test_bets_dataset(spark):
    """Test the Bets dataset class"""
    bets_dataset = bets.Bets()
    assert bets_dataset.name == "Bets"
    # Expected number of records
    df = bets_dataset.reader(data_points=1000)
    assert df.count() == 1000
    # Expected schema
    expected_columns = {"market", "odds", "timestamp", "bet_amount", "player_id"}
    assert all(col in df.columns for col in expected_columns)
    # Expected number of records under default behavior
    df = bets_dataset.reader()
    assert df.count() == 1000000


def test_numbers_dataset(spark):
    """Test the Numbers dataset class"""
    numbers_dataset = numbers.Numbers()
    assert numbers_dataset.name == "Numbers"
    # Expected number of records
    df = numbers_dataset.reader(n=1000)
    assert df.count() == 1000
    # Expected schema
    df.show(truncate=False)
    expected_columns = {"n"}
    assert all(col in df.columns for col in expected_columns)
    # Expected number of records under default behavior
    df = numbers_dataset.reader()
    assert df.count() == 10
