"""Stripe tap class."""

from pathlib import Path
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

# TODO: Import your custom stream types here:
from tap_stripe.streams import (
    StripeStream,
    CustomersStream,
    PlansStream,
    SubscriptionsStream,
)

# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    CustomersStream,
    PlansStream,
    SubscriptionsStream,
]


class TapStripe(Tap):
    """Stripe tap class."""

    name = "tap-stripe"

    # TODO: Accept JSONSchemas for metadata
    config_jsonschema = PropertiesList(
        Property("api_key", StringType, required=True),
        Property("start_date", DateTimeType, required=True),
        Property("account_id", StringType),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


# CLI Execution:

cli = TapStripe.cli
