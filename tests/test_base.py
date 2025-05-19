from beyond_bets.base import dataset, transform


def test_dataset_abstract_class():
    """Test that the Dataset class is an abstract class with expected methods"""
    # Check that the Dataset class has a name property
    assert hasattr(dataset.Dataset, "name")
    # Check that the Dataset class has a constructor
    assert hasattr(dataset.Dataset, "__init__")
    # Check that the Dataset class has a reader method
    assert hasattr(dataset.Dataset, "reader")
    # Check that the Dataset class has a writer method
    assert hasattr(dataset.Dataset, "writer")
    # Check expected abstract methods
    assert dataset.Dataset.__abstractmethods__ == {"reader", "writer"}


def test_transform_abstract_class():
    """Test that the Transform class is an abstract class with expected methods"""
    # Check that the Transform class has a name property
    assert hasattr(transform.Transform, "name")
    # Check that the Transform class has a constructor
    assert hasattr(transform.Transform, "__init__")
    # Check that the Transform class has an ingest method
    assert hasattr(transform.Transform, "_ingest")
    # Check that the Transform class has a transformation method
    assert hasattr(transform.Transform, "_transformation")
    # Check that the Transform class has a result method
    assert hasattr(transform.Transform, "result")
    # Check expected abstract methods
    assert transform.Transform.__abstractmethods__ == {"_transformation"}
