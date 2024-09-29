import pytest
import pandas as pd
import os
from unittest import mock
from src.handlers.db_handlers.mongo_handler import MongoDBHandler


@pytest.fixture
def mongo_handler():
    """
    Fixture to create an instance of MongoDBHandler for testing.
    """
    # Mock environment variables for MongoDB connection
    with mock.patch.dict(os.environ, {
        "MONGODB_URI": "mongodb://mocked_uri:27017/",
        "MONGODB_DATABASE": "test_database",
        "MONGODB_COLLECTION": "test_collection"
    }):
        yield MongoDBHandler()


def test_mongo_handler_init(mongo_handler):
    """
    Test MongoDBHandler initialization.
    Ensure that the handler is using the correct MongoDB URI, database, and collection.
    """
    assert mongo_handler.client is not None
    assert mongo_handler.db.name == "test_database"
    assert mongo_handler.collection.name == "test_collection"


@mock.patch("pymongo.collection.Collection.insert_many")
def test_insert_records_success(mock_insert_many, mongo_handler):
    """
    Test MongoDBHandler's insert_records method for successful insertion.
    Verify that insert_many is called and logs correctly.
    """
    # Sample records to insert
    records = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]

    # Call insert_records
    mongo_handler.insert_records(records)

    # Ensure insert_many is called once
    mock_insert_many.assert_called_once_with(records)

    # Check logger outputs (assuming INFO logging level)
    with mock.patch.object(mongo_handler.logger, "info") as mock_log_info:
        mongo_handler.insert_records(records)
        mock_log_info.assert_any_call(f"Inserting data into MongoDB collection {mongo_handler.collection.name}")
        mock_log_info.assert_any_call("Data successfully inserted into MongoDB")


@mock.patch("pymongo.collection.Collection.insert_many")
def test_insert_records_failure(mock_insert_many, mongo_handler):
    """
    Test MongoDBHandler's insert_records method for failure scenario.
    Verify that logging occurs on failure.
    """
    # Sample records to insert
    records = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]

    # Simulate an exception during insert_many
    mock_insert_many.side_effect = Exception("Mocked insert failure")

    with mock.patch.object(mongo_handler.logger, "error") as mock_log_error:
        with pytest.raises(Exception, match="Mocked insert failure"):
            mongo_handler.insert_records(records)
        mock_log_error.assert_called_once_with("Failed to insert data into MongoDB: Mocked insert failure")


def test_save_data(mongo_handler):
    """
    Test MongoDBHandler's save_data method.
    Verify that DataFrame is converted to records and inserted.
    """
    # Create a DataFrame
    df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [24, 30]})

    # Mock insert_records to verify it is called
    with mock.patch.object(mongo_handler, "insert_records") as mock_insert_records:
        mongo_handler.save_data(df)
        # Verify that insert_records was called with the correct data
        records = df.to_dict(orient="records")
        mock_insert_records.assert_called_once_with(records)

