"""Stream class for tap-stripe."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

import stripe
from singer_sdk.streams import Stream
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

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

STREAM_ENDPOINTS = {
    "balance_transactions": stripe.BalanceTransaction,
    "charges": stripe.Charge,
    "coupons": stripe.Coupon,
    "customers": stripe.Customer,
    "disputes": stripe.Dispute,
    "events": stripe.Event,
    "invoices": stripe.Invoice,
    "payment_intents": stripe.PaymentIntent,
    "payouts": stripe.Payout,
    "plans": stripe.Plan,
    "promotion_codes": stripe.PromotionCode,
    "refunds": stripe.Refund,
    "subscriptions": stripe.Subscription,
    "subscription_schedules": stripe.SubscriptionSchedule
}


class StripeStream(Stream):
    """Stream class for Stripe streams."""

    def get_records(self, partition: Optional[dict] = None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `partition` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `partitions` argument.
        """

        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        raise NotImplementedError("The method is not yet implemented (TODO)")


# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.
class UsersStream(StripeStream):
    name = "users"

    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = PropertiesList(
        Property("name", StringType),
        Property("id", StringType),
        Property("age", IntegerType),
        Property("email", StringType),
        Property("street", StringType),
        Property("city", StringType),
        Property("state", StringType),
        Property("zip", StringType),
    ).to_dict()


class GroupsStream(StripeStream):
    name = "groups"

    primary_keys = ["id"]
    replication_key = "modified"
    schema = PropertiesList(
        Property("name", StringType),
        Property("id", StringType),
        Property("modified", DateTimeType),
    ).to_dict()
