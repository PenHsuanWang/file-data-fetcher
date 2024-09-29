import pytest
import os
import pandas as pd
from unittest import mock
from pydantic import ValidationError
from hashlib import md5
from src.handlers.file_fetch_handlers.csv_fetch_handler import CSVFetchHandler
from src.handlers.file_fetch_handlers.excel_fetch_handler import ExcelFetchHandler
from src.handlers.file_fetch_handlers.base_file_fetcher_handler import BaseFileFetchHandler
from src.models.data_record import DataRecord


@pytest.fixture
def processed_registry_mock():
    """
    Fixture to create a mock of the ProcessedFilesRegistry.
    """
    return mock.MagicMock()


@pytest.fixture
def csv_handler(processed_registry_mock):
    """
    Fixture to create an instance of CSVFetchHandler with a mock registry.
    """
    return CSVFetchHandler(processed_registry_mock)


@pytest.fixture
def excel_handler(processed_registry_mock):
    """
    Fixture to create an instance of ExcelFetchHandler with a mock registry.
    """
    return ExcelFetchHandler(processed_registry_mock)


# Helper function to mock os.path.getsize for file readiness
def mock_getsize(file_sizes):
    """
    Mocks os.path.getsize by returning a list of sizes in sequence.
    """
    def side_effect(file_path):
        return file_sizes.pop(0)
    return mock.patch('os.path.getsize', side_effect=side_effect)


# Helper function to mock file content reading
def mock_open_file(content):
    """
    Mocks open() and returns binary content when file is read.
    """
    if isinstance(content, str):
        content = content.encode()  # Ensure content is treated as binary
    return mock.patch('builtins.open', mock.mock_open(read_data=content))


# Test cases for BaseFileFetchHandler (shared functionality)

def test_is_file_ready_when_size_stable(csv_handler):
    """
    Test file readiness when the file size is stable (not changing).
    """
    with mock_getsize([100, 100]):
        assert csv_handler.is_file_ready("dummy.csv") is True


def test_is_file_ready_when_size_changes(csv_handler):
    """
    Test file readiness when the file size is changing.
    """
    with mock_getsize([100, 150]):
        assert csv_handler.is_file_ready("dummy.csv") is False


def test_calculate_checksum(csv_handler):
    """
    Test checksum calculation for a file.
    """
    file_content = b"some file content"  # Change to binary content
    expected_checksum = md5(file_content).hexdigest()  # No need to encode again
    
    with mock_open_file(file_content), mock_getsize([len(file_content)]):
        checksum = csv_handler.calculate_checksum("dummy.csv")
        assert checksum == expected_checksum


def test_calculate_checksum_failure(csv_handler):
    """
    Test checksum calculation when the file can't be read.
    """
    with mock.patch('builtins.open', mock.Mock(side_effect=FileNotFoundError)):
        assert csv_handler.calculate_checksum("dummy.csv") is None


# Test cases for CSVFetchHandler
def test_csv_process_file_success(csv_handler, processed_registry_mock, mocker):
    """
    Test successful processing of a CSV file with valid data.
    """
    processed_registry_mock.is_processed.return_value = False  # Ensure file is not marked as processed
    file_content = "col1,col2\nvalue1,value2\n"
    mock_df = pd.DataFrame([{"col1": "value1", "col2": "value2"}])

    # Mock CSV reading
    mocker.patch('pandas.read_csv', return_value=mock_df)
    # Mock validation success
    mocker.patch('src.models.data_record.DataRecord', return_value=mock.MagicMock(dict=lambda: mock_df.to_dict(orient='records')))

    # Use binary content for checksum calculation
    binary_file_content = b"col1,col2\nvalue1,value2\n"
    with mock_open_file(binary_file_content), mock_getsize([100, 100]):
        result = csv_handler.process_file("dummy.csv")

        assert result is not None
        records, filename, checksum = result
        assert records == mock_df.to_dict(orient='records')
        assert filename == "dummy.csv"
        assert checksum is not None



def test_csv_process_file_already_processed(csv_handler, processed_registry_mock):
    """
    Test that an already processed file is skipped.
    """
    processed_registry_mock.is_processed.return_value = True

    result = csv_handler.process_file("dummy.csv")
    
    assert result is None
    processed_registry_mock.is_processed.assert_called_once_with("dummy.csv")


from pydantic import ValidationError
from pydantic import BaseModel

def test_csv_process_file_validation_error(csv_handler, processed_registry_mock, mocker):
    """
    Test processing of a CSV file when data validation fails.
    """
    processed_registry_mock.is_processed.return_value = False  # Ensure the file is not already processed

    file_content = "col1,col2\nvalue1,value2\n"
    mock_df = pd.DataFrame([{"col1": "value1", "col2": "value2"}])

    # Mock CSV reading
    mocker.patch('pandas.read_csv', return_value=mock_df)

    # Create a mock validation error using Pydantic v2's ValidationError
    class MockDataRecord(BaseModel):
        col1: str
        col2: str
    
    validation_error = ValidationError.from_exception_data(
        MockDataRecord, [{'loc': ('col1',), 'msg': 'Invalid value', 'type': 'value_error'}]
    )

    # Mock DataRecord to raise the validation error
    mocker.patch('src.models.data_record.DataRecord', side_effect=validation_error)

    with mock_open_file(file_content), mock_getsize([100, 100]):
        result = csv_handler.process_file("dummy.csv")
        
        assert result is None



def test_excel_process_file_success(excel_handler, processed_registry_mock, mocker):
    """
    Test successful processing of an Excel file with valid data.
    """
    processed_registry_mock.is_processed.return_value = False  # Ensure file is not marked as processed
    mock_df = pd.DataFrame([{"col1": "value1", "col2": "value2"}])

    # Mock Excel reading
    mocker.patch('pandas.read_excel', return_value=mock_df)
    # Mock validation success
    mocker.patch('src.models.data_record.DataRecord', return_value=mock.MagicMock(dict=lambda: mock_df.to_dict(orient='records')))

    # Use binary content for checksum calculation
    binary_file_content = b"col1,col2\nvalue1,value2\n"
    with mock_open_file(binary_file_content), mock_getsize([100, 100]):
        result = excel_handler.process_file("dummy.xlsx")

        assert result is not None
        records, filename, checksum = result
        assert records == mock_df.to_dict(orient='records')
        assert filename == "dummy.xlsx"
        assert checksum is not None




def test_excel_process_file_already_processed(excel_handler, processed_registry_mock):
    """
    Test that an already processed Excel file is skipped.
    """
    processed_registry_mock.is_processed.return_value = True

    result = excel_handler.process_file("dummy.xlsx")
    
    assert result is None
    processed_registry_mock.is_processed.assert_called_once_with("dummy.xlsx")



def test_excel_process_file_validation_error(excel_handler, processed_registry_mock, mocker):
    """
    Test processing of an Excel file when data validation fails.
    """
    processed_registry_mock.is_processed.return_value = False  # Ensure the file is not already processed

    mock_df = pd.DataFrame([{"col1": "value1", "col2": "value2"}])

    # Mock Excel reading
    mocker.patch('pandas.read_excel', return_value=mock_df)

    # Create a mock validation error using Pydantic v2's ValidationError
    class MockDataRecord(BaseModel):
        col1: str
        col2: str
    
    validation_error = ValidationError.from_exception_data(
        MockDataRecord, [{'loc': ('col1',), 'msg': 'Invalid value', 'type': 'value_error'}]
    )

    # Mock DataRecord to raise the validation error
    mocker.patch('src.models.data_record.DataRecord', side_effect=validation_error)

    with mock_getsize([100, 100]):
        result = excel_handler.process_file("dummy.xlsx")
        
        assert result is None

