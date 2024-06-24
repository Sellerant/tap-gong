from typing import List
import singer
import singer.utils as singer_utils
from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_gong.streams import (
    CallsStream,
    CallTranscriptsStream,
    UsersStream,
)
import json

LOGGER = singer.get_logger()
STREAM_TYPES = [
    CallsStream,
    CallTranscriptsStream,
    UsersStream
]
REQUIRED_CONFIG_KEYS = ['access_token',
                        'user_agent',
                        'start_date']

CONFIG = {
    'start_date': None
}


class TapGong(Tap):
    """Gong tap class."""
    name = "tap-gong"

    config_jsonschema = th.PropertiesList(
        th.Property("access_token", th.StringType, required=True),
        th.Property("user_agent", th.StringType, required=True),
        th.Property("start_date", th.DateTimeType, default=None),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


def main_impl():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    
    catalog = None
    if args.catalog:
        with open(args.catalog, 'r', encoding='utf-8') as catalog_file:
            catalog = json.load(catalog_file)

    # Create an instance of TapGong with the configuration
    go = TapGong(config=CONFIG, catalog=catalog)

    # Run the discovery if the --discover argument is passed
    if args.discover:
        go.run_discovery()
    else:
        # Otherwise, run the tap normally
        go.sync_all()


def main():
    try:
        main_impl()
    except Exception as e:
        LOGGER.critical(e)


if __name__ == "__main__":
    main()