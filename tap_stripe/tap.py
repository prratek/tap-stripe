"""Stripe tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk.typing import DateTimeType, PropertiesList, Property, StringType

from tap_stripe.streams import (
    ChargesStream,
    CustomersStream,
    InvoicesStream,
    PayoutsStream,
    PlansStream,
    PromotionCodesStream,
    SubscriptionsStream,
)

STREAM_TYPES = [
    # ChargesStream,
    # CustomersStream,
    # InvoicesStream,
    PayoutsStream,
    # PlansStream,
    # PromotionCodesStream,
    # SubscriptionsStream,
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
