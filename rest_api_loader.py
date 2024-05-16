import dlt
import requests
import json
import logging
import sys
import base64
from datetime import datetime, timedelta
from rest_api import RESTAPIConfig, rest_api_source
from token_retriever.retriever import AWSTokenRetriever
from toml_updater.updater import TomlConfigUpdater

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataLoader:
    """
    DataLoader handles loading data from a REST API into a destination database using DLT pipelines based on a configuration file.

    Attributes:
        base_url (str): The base URL of the REST API.
        token (str): Authorization token for the REST API.
        config_path (str): Path to the JSON configuration file.
    """
    def __init__(self, base_url: str, token: str, dataset: str, config_path: str):
        self.base_url = base_url
        self.token = token
        self.dataset = dataset
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        """Loads configuration from a JSON file."""
        try:
            with open(self.config_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            logging.error(f"Failed to load config: {e}")
            raise

    def _create_pipeline(self) -> dlt.Pipeline:
        """Creates and returns a DLT pipeline configured for a specific dataset."""
        return dlt.pipeline(
            pipeline_name=f"{self.dataset}_pipeline",
            destination="athena",
            staging="filesystem",
            dataset_name=self.dataset,
        )

    def _get_api_config(self, resource: dict, date_from: str, date_to: str, load_type: str) -> dict:
        """
        Generates and returns the REST API configuration for a specified resource and date range,
        incorporating dynamic settings for cursor path and load type, and selectively includes primary key based on load type.
        
        Args:
            resource (dict): The resource configuration.
            date_from (str): The starting date for the data load.
            date_to (str): The ending date for the data load.
            load_type (str): The intended type of load, either 'full' or 'incremental'.
            
        Returns:
            dict: The configuration dictionary for use with the REST API.
        """
        try:
            logging.info(f"Initial load type requested by docker call: {load_type}")

            always_full = resource["params"].get("always_full", False)
            if always_full:
                load_type = 'full' 
                logging.info(f"Resource {resource['name']} is configured for always full load. Overriding to full load.")

            write_disposition = "replace"
            where_clause = resource["params"].get("where", "1=1")
            
            resource_config = {
                "name": resource["name"],
                "endpoint": {
                    "params": {
                        "where": where_clause,
                        "from_date": "1970-01-01" if load_type == 'full' else date_from,
                        "to_date": date_to                    
                    }
                },
                "write_disposition": write_disposition
            }

            logging.info(f"Configuring {load_type} load for {resource['name']} with: {resource_config['endpoint']['params']}")

            return {
                "client": {
                    "base_url": self.base_url,
                    "headers": {
                        'Authorization': f'Basic {base64.b64encode(self.token.encode()).decode()}',
                        "Content-Type": "application/json"
                    }
                },
                "resource": resource_config
            }
        except KeyError as e:
            logging.error(f"Configuration error for {resource['name']}: {e}")
            raise

    def _create_fetch_api_data(self, base_url: str, headers: dict, params: dict):
        """
        Creates a generator function to fetch data from an API that returns data in JSONL format.

        Args:
            base_url (str): The base URL of the REST API.
            headers (dict): The headers for the REST API request.
            params (dict): The parameters for the REST API request.
        
        Returns:
            function: A decorated generator function.
        """
        @dlt.resource()
        def fetch_api_data():
            response = requests.get(base_url, headers=headers, params=params, stream=True)
            if response.status_code != 200:
                raise ValueError(f"API request failed with status: {response.status_code}")

            for line in response.iter_lines():
                if line:
                    yield json.loads(line.decode('utf-8'))
        
        return fetch_api_data

    def load_data_for_resource(self, resource_name: str, date_from: str, date_to: str, load_type: str) -> None:
        """Loads data for a specific resource and date range."""
        resource_config = next((item for item in self.config['resources'] if item['name'] == resource_name), None)
        if not resource_config:
            logging.error(f"No configuration found for resource: {resource_name}")
            return
        pipeline = self._create_pipeline()
        api_config = self._get_api_config(resource_config, date_from, date_to, load_type)
        headers = api_config["client"]["headers"]
        params = api_config["resource"]["endpoint"]["params"]
        try:
            fetch_api_data = self._create_fetch_api_data(self.base_url, headers, params)
            result = pipeline.run(
                data=fetch_api_data,
                table_name=resource_name)
            logging.info(f"Load successful for {resource_name}: {result}")
        except Exception as e:
            logging.error(f"Failed to load data for {resource_name}: {e}")

    def run_full_load(self):
        """Executes a full load of data up to the end of the previous day for all resources."""
        yesterday = datetime.now() - timedelta(days=1)
        date_to = yesterday.strftime('%Y-%m-%dT23:59:59Z')
        for resource in self.config['resources']:
            logging.info(f"Running full load for {resource['name']}")
            self.load_data_for_resource(resource['name'], "1970-01-01T00:00:00Z", date_to, "full")

    def run_incremental_load(self):
        """Executes an incremental load of data for the last day for all resources."""
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        date_from = yesterday.strftime('%Y-%m-%d')
        date_to = yesterday.strftime('%Y-%m-%d')
        for resource in self.config['resources']:
            logging.info(f"Running incremental load for {resource['name']}")
            self.load_data_for_resource(resource['name'], date_from, date_to, "incremental")

if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in ['full', 'incremental']:
        logging.error("Usage: python data_loader.py [full|incremental] [s3_url]")
        sys.exit(1)
        
    config_path = "../configs/configs.json"
    secrets_path = "./.dlt/secrets.toml"
        
    token = AWSTokenRetriever("/some//token").retrieve_token_from_ssm()
    s3_bucket = TomlConfigUpdater(secrets_path).update_bucket_url(sys.argv[2])

    loader = DataLoader(
        base_url="https://example/api/2.0/my_endpoint/",
        token=token,
        dataset="my_dataset",
        config_path=config_path)

    if sys.argv[1] == 'full':
        loader.run_full_load()
    elif sys.argv[1] == 'incremental':
        loader.run_incremental_load()
