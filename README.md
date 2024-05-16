# DataLoader Script

## Overview
The `DataLoader` script is designed to load data from a REST API into a destination database using DLT pipelines based on a configuration file. The script supports both full and incremental data loads, ensuring that data is consistently replaced with the latest available data.

## Key Components
- **DataLoader Class**: Handles the entire data loading process.
- **DLT Pipeline**: Uses DLT (Data Load Tool) to manage data pipelines.
- **REST API Integration**: Fetches data from a REST API that returns data in JSONL (JSON Lines) format.

## Dependencies
- `dlt`: Data Load Tool for managing data pipelines.
- `requests`: For making HTTP requests to the REST API.
- `logging`: For logging information, warnings, and errors.
- `sys`, `base64`, `datetime`, `timedelta`: Standard Python libraries.
- `rest_api`, `token_retriever`, `toml_updater`: Custom modules for API configuration, token retrieval, and configuration updates.

## How to Run the Script
```bash
python data_loader.py [full|incremental] [s3_url]
```
- `full` or `incremental`: Specifies whether to perform a full load or an incremental load.
- `s3_url`: The S3 URL where the data will be stored.

## Configuration
- **config_path**: Path to the JSON configuration file (`../configs/configs.json`).
- **secrets_path**: Path to the secrets configuration file (`./.dlt/secrets.toml`).

## Detailed Description of the Script

### DataLoader Class
The `DataLoader` class manages the loading of data from a REST API into a destination database using DLT pipelines. It is initialized with the following attributes:
- `base_url`: The base URL of the REST API.
- `token`: Authorization token for the REST API.
- `dataset`: The name of the dataset.
- `config_path`: Path to the JSON configuration file.

### Methods

#### `_load_config`
Loads the configuration from a JSON file.

#### `_create_pipeline`
Creates and returns a DLT pipeline configured for a specific dataset.

#### `_get_api_config`
Generates and returns the REST API configuration for a specified resource and date range. This configuration includes the endpoint parameters and headers required for the API request.

#### `_create_fetch_api_data`
Creates a generator function to fetch data from an API that returns data in JSONL format.

### `fetch_api_data` Decorator
The `fetch_api_data` method is decorated with `@dlt.resource()`, indicating that it is a data source for the DLT pipeline. This function makes an HTTP GET request to the specified API endpoint and yields each line of JSONL data as a JSON object.

#### `load_data_for_resource`
Loads data for a specific resource and date range. It performs the following steps:
1. Fetches the API configuration.
2. Creates the fetch API data generator.
3. Runs the DLT pipeline, replacing the existing data in the table with the new data.

#### `run_full_load`
Executes a full load of data up to the end of the previous day for all resources specified in the configuration file.

#### `run_incremental_load`
Executes an incremental load of data for the last day for all resources specified in the configuration file.

### Main Execution
The script accepts command-line arguments to determine the load type (`full` or `incremental`) and the S3 URL for data storage. Based on the load type, it either runs a full load or an incremental load.

## Example Configuration File (`configs.json`)
```json
{
    "resources": [
        {
            "name": "resource_1",
            "params": {
                "always_full": false,
                "where": "1=1"
            }
        },
        {
            "name": "resource_2",
            "params": {
                "always_full": true,
                "where": "status = 'active'"
            }
        }
    ]
}
```

## Handling of JSONL API Responses
The `fetch_api_data` function handles JSONL responses from the API. JSONL (JSON Lines) format is a convenient format for streaming JSON objects. Each line in a JSONL file is a valid JSON object.

### `fetch_api_data` Implementation
```python
@dlt.resource()
def fetch_api_data():
    response = requests.get(base_url, headers=headers, params=params, stream=True)
    if response.status_code != 200:
        raise ValueError(f"API request failed with status: {response.status_code}")

    for line in response.iter_lines():
        if line:
            yield json.loads(line.decode('utf-8'))
```
- **`@dlt.resource()` Decorator**: This decorator indicates that `fetch_api_data` is a data source for the DLT pipeline.
- **Streaming Data**: The `stream=True` parameter in the `requests.get` call enables streaming of the response, allowing the function to handle large datasets efficiently.
- **Line-by-Line Processing**: The function processes each line of the response, decoding it and yielding it as a JSON object.

### Error Handling
The script includes error handling for configuration loading, API requests, and data loading operations. Errors are logged with appropriate messages to help diagnose issues.


The `DataLoader` script offer a solution for loading data from a REST API into a destination database using DLT pipelines. It ensures data consistency by replacing the existing data with each load, making it suitable for both full and incremental data loads. The use of the `@dlt.resource()` decorator and efficient handling of JSONL responses make it a scalable and efficient solution for data integration tasks.
