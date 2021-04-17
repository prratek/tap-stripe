"""Stream class for tap-stripe."""

from pathlib import Path
from typing import Iterable, Optional

import stripe
from singer_sdk.streams import Stream
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
from stripe.api_resources.abstract import (
    ListableAPIResource as StripeListableAPIResource,
)
from stripe.api_resources.list_object import ListObject as StripeListObject

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

SDK_OBJECTS = {
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
    "subscription_schedules": stripe.SubscriptionSchedule,
}

EVENT_TYPE_FILTERS = {
    "charges": {
        "types": [
            "charge.captured",
            "charge.expired",
            "charge.failed",
            "charge.pending",
            "charge.refunded",
            "charge.succeeded",
            "charge.updated",
        ]
    },
    "customers": {
        "types": ["customer.created", "customer.deleted", "customer.updated"]
    },
    "invoices": {"type": "invoice.*"},
    "payouts": {"type": "payout.*"},
    "plans": {"type": "plan.*"},
    "promotion_codes": {"type": "promotion_code.*"},
    "subscriptions": {"type": "customer.subscription.*"},
}


class StripeStream(Stream):
    """Stream class for Stripe streams."""

    @property
    def sdk_object(self) -> StripeListableAPIResource:
        return (
            stripe.Event
            if self.replication_method == REPLICATION_INCREMENTAL
            else SDK_OBJECTS[self.name]
        )

    def _make_created_filter(self) -> dict:
        return {"gte": self.get_starting_timestamp(partition=None)}

    def _make_params(self, limit: int = 100) -> dict:
        if self.replication_method == REPLICATION_INCREMENTAL:
            type_filter = EVENT_TYPE_FILTERS[self.name]
            other_filters = {"created": self._make_created_filter(), "limit": limit}
            return {**type_filter, **other_filters}

        elif self.replication_method == REPLICATION_FULL_TABLE:
            if self.name == "subscriptions":
                return {
                    "created": self._make_created_filter(),
                    "limit": limit,
                    "status": "all",
                }
            else:
                return {"created": self._make_created_filter(), "limit": limit}

        else:
            raise ValueError

    def _get_iterator(self, limit: int = 100) -> StripeListObject:
        params = self._make_params(limit=limit)
        return self.sdk_object.list(**params)

    def get_records(self, partition: Optional[dict] = None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `partition` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `partitions` argument.
        """

        stripe.api_key = self._config["api_key"]
        iterator = self._get_iterator()

        for row in iterator.auto_paging_iter():
            yield row.to_dict()


class ChargesStream(StripeStream):
    """Stripe Plans stream"""

    name = "charges"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "charges.schema.json"


class CustomersStream(StripeStream):
    """Stripe Plans stream"""

    name = "customers"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "customers.schema.json"


class InvoicesStream(StripeStream):
    """Stripe Plans stream"""

    name = "invoices"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "invoices.schema.json"


class PayoutsStream(StripeStream):
    """Stripe Plans stream"""

    name = "payouts"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "payouts.schema.json"


class PlansStream(StripeStream):
    """Stripe Plans stream"""

    name = "plans"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "shared/plans.schema.json"


class PromotionCodesStream(StripeStream):
    """Stripe Plans stream"""

    name = "promotion_codes"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "promotion-codes.schema.json"


class SubscriptionsStream(StripeStream):
    """Stripe Subscriptions stream."""

    name = "subscriptions"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "subscriptions.schema.json"
