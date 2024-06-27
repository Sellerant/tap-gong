import argparse
import json
import logging
from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
import singer_sdk.exceptions
from tap_gong.streams import CallsStream, CallTranscriptsStream, UsersStream

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

STREAM_TYPES = [CallsStream, CallTranscriptsStream, UsersStream]

REQUIRED_CONFIG_KEYS = ['access_token', 'user_agent', 'start_date']

CONFIG = {'start_date': None}

class TapGong(Tap):
    """Gong tap class."""
    name = "tap-gong"

    config_jsonschema = th.PropertiesList(
        th.Property("access_token", th.StringType, required=True),
        th.Property("user_agent", th.StringType, required=True),
        th.Property("start_date", th.DateTimeType, default=None),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        config_helper.extended_config_validation(self.config)

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

def main_impl(args):
    CONFIG.update(args.config)

    catalog = None
    if args.catalog:
        with open(args.catalog, 'r', encoding='utf-8') as catalog_file:
            catalog = json.load(catalog_file)

    # Create an instance of TapGong with the configuration
    go = TapGong(config=CONFIG, catalog=catalog)

    try:
        if args.discover:
            go.run_discovery()
        else:
            go.sync_all()

    except singer_sdk.exceptions.FatalAPIError as e:
        LOGGER.error(f"Fatal API error occurred: {str(e)}")
        raise

    except Exception as e:
        LOGGER.error(f"An unexpected error occurred: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Tap Gong')
    parser.add_argument('--config', required=True, help='Configuration file')
    parser.add_argument('--catalog', help='Catalog file')
    parser.add_argument('--discover', action='store_true', help='Run discovery mode')
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as config_file:
        args.config = json.load(config_file)

    main_impl(args)

if __name__ == "__main__":
    main()
