import json
import logging

from typing import Dict, List

from schematools.bq_types import *
from schematools.data_detectors import *
from schematools.exceptions import BQSchemaMergeException
from schematools.schema_merge import merge_schemas

PRIMITIVE_TYPES = [bool, float, int, str]
logger = logging.getLogger()


def extract_schema(json_data: str) -> List[object]:
    """
    Extracts schema from a json object.
    Schema is a list of BigQuery schema objects,
    one for every field.

    :param json_data: json string
    :return: a schema
    """
    # Convert to Python dict
    data = json.loads(json_data)

    # FUTURE add validation checks

    # Extract schema (provide dummy name for top-level)
    schema = convert_to_bq_schema_complex('toplevel', data)

    # Return top-level fields
    return schema['fields']


def convert_to_bq_schema_complex(field_name: str, field_value: Dict[str, object]) -> Dict[str, object]:
    """
    Converts a complex object into a 'RECORD' BigQuery schema field.
    The type is set to 'RECORD', the mode is set to 'REQUIRED'.

    :param field_name: name of the resulting field
    :param field_value: dictionary containing the child objects
    :return: a 'RECORD' schema entry
    """
    fields = []
    for child_field_name, child_field_value in field_value.items():
        # Null fields are skipped
        if child_field_value is None:
            pass
        # Convert primitive fields to schema
        elif is_primitive(child_field_value):
            field = convert_to_bq_schema_primitive(child_field_name, child_field_value)
            fields.append(field)
        # Convert lists to schema
        elif isinstance(child_field_value, list):
            # Skip empty lists
            if len(child_field_value) > 0:
                field = convert_to_bq_schema_list(child_field_name, child_field_value)
                fields.append(field)
        # Convert complex fields to schema
        else:
            field = convert_to_bq_schema_complex(child_field_name, child_field_value)
            fields.append(field)

    return {
        'name': field_name,
        'type': TYPE_RECORD,
        'mode': MODE_REQUIRED,
        'fields': fields,
    }


def convert_to_bq_schema_list(field_name: str, field_values: List[object]) -> Dict[str, object]:
    """
    Converts a list of objects into a 'REPEATED' BigQuery schema field.
    The mode is set to 'REPEATED', the type is based on the schema of the elements.

    The schemas for internal elements are merged together
    :param field_name: name of the resulting field
    :param field_values: list of values contained in this field
    :return: a 'REPEATED' schema entry
    """
    field_schemas = []
    for field_value in field_values:
        # Null fields are skipped
        if field_value is None:
            pass
        # Convert primitive fields to schema
        elif is_primitive(field_value):
            field_schema = convert_to_bq_schema_primitive(field_name, field_value)
            field_schemas.append(field_schema)
        # Nested lists are not allowed
        elif isinstance(field_value, list):
            raise BQSchemaMergeException(f'Nested arrays are not supported: array field {field_name}')
        # Convert complex fields to schema
        else:
            field_schema = convert_to_bq_schema_complex(field_name, field_value)
            field_schemas.append(field_schema)

    # Merge all schemas
    final_schema_container = None
    for current_schema in field_schemas:
        # Wrap new schema in a list, so it resembles a schema of one field
        # Then merge it against the accumulated schema
        final_schema_container = merge_schemas(final_schema_container, [current_schema])

    # Final schema should have one field
    if not final_schema_container or len(final_schema_container) != 1:
        raise BQSchemaMergeException(f'Failed to merge schemas of array field {field_name}')
    final_schema = final_schema_container[0]

    # Set the resulting field to repeated
    final_schema['mode'] = MODE_REPEATED

    # Return merged schema
    return final_schema


def convert_to_bq_schema_primitive(field_name: str, field_value: object) -> Dict[str, object]:
    """
    Converts a primitive value into a primitive, required BigQuery schema field.

    :param field_name: name of the resulting field
    :param field_value: a primitive value
    :return: a 'REQUIRED' schema entry with a primitive type
    """
    return {
        'name': field_name,
        'type': determine_field_type(field_value),
        'mode': MODE_REQUIRED,
    }


def is_primitive(field_value):
    """
    Check is a value is primitive.

    :param field_value: the value of the field
    :return: True iff the value is a primitive one
    """
    return type(field_value) in PRIMITIVE_TYPES


def determine_field_type(field_value: object) -> str:
    """
    Extracts the field type of a value based on its Python type.

    :param field_value: the value of the field
    :return: the inferred BigQuery field type, as a string
    """

    if field_value is None:
        return TYPE_STRING
    elif isinstance(field_value, bool):
        return TYPE_BOOLEAN
    elif isinstance(field_value, float):
        return TYPE_FLOAT
    elif isinstance(field_value, int):
        return TYPE_INTEGER
    elif isinstance(field_value, str):
        # Check for specialized string fields
        # FUTURE use a "data detector" system that uses injection
        if is_timestamp(field_value):
            return TYPE_TIMESTAMP
        elif is_date(field_value):
            return TYPE_DATE
        elif is_time(field_value):
            return TYPE_TIME

        return TYPE_STRING

    logger.warning(f'Failed to detect type of {field_value}, falling back to {TYPE_STRING} type...')

    # Fall back to string type
    return TYPE_STRING

