"""Tests loading and validity of stream schemas"""

from pathlib import Path
from typing import Dict, List, Union

from singer_sdk.helpers._util import read_json_file

from tap_stripe.tap import TapStripe

SCHEMAS_DIR = "tap_stripe/schemas"
SCHEMA_FIELD_TYPES = ["object", "array", "string", "number", "null"]


def _list_schema_paths(schemas_dir: str) -> List[Path]:
    return sorted(Path(schemas_dir).glob("**/*.schema.json"))


def _field_type_is_valid(field_type: List[str]) -> bool:
    # field type must be a list
    if not isinstance(field_type, list):
        return False
    # types in the list should be unique
    if len(field_type) > len(set(field_type)):
        return False
    # check that each element in the list is an accepted "type"
    for type_val in field_type:
        if type_val not in SCHEMA_FIELD_TYPES:
            return False
    return True


def _schema_fields_have_explicit_types(schema: Dict[str, Union[Dict, List]]) -> bool:
    properties = schema.get("properties")
    if not properties or not isinstance(properties, dict):
        return False

    for field_name, field_val in properties.items():
        # schema fields must declare a type
        if not field_val.get("type"):
            return False
        # the type declaration must meet some requirements to be "valid"
        if not _field_type_is_valid(field_val.get("type")):
            return False
        # for object types, make a recursive call to ensure properties have explicit types
        if "object" in field_val.get("type"):
            return _schema_fields_have_explicit_types(field_val)
        # finally, check that the scalar field has an explicit type
        return any(
            t in SCHEMA_FIELD_TYPES for t in field_val.get("type") if t != "null"
        )


def test_schemas_load():
    """Test that all schema files load and parse successfully"""
    schema_files = _list_schema_paths(SCHEMAS_DIR)
    for schema_file in schema_files:
        assert read_json_file(schema_file)


def test_fields_have_explicit_types():
    """Test that all fields have fully specified types - typically required for warehouse targets"""
    schema_files = _list_schema_paths(SCHEMAS_DIR)
    for schema_file in schema_files:
        schema = read_json_file(schema_file)
        assert _schema_fields_have_explicit_types(schema)
