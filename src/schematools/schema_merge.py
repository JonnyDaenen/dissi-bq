from schematools.bq_types import *
from schematools.exceptions import BQSchemaMergeException
from typing import Dict, List


def merge_schemas(schema1: List[Dict], schema2: List[Dict]):
    """
    Merges two schemas into a relaxed one that is compatible with both.
    Schemas are provided as a list of field schemas.

    :param schema1: first BigQuery schema
    :param schema2: second BigQuery schema
    :return: merged BigQuery schema
    """
    if schema1 is None:
        return schema2
    if schema2 is None:
        return schema1

    return merge_schema_fields_rec(schema1, schema2)


def merge_schema_fields_rec(schema1: List[Dict], schema2: List[Dict]) -> List[Dict]:
    """
    Recursively merges two lists of field schemas.

    :param schema1: first BigQuery schema
    :param schema2: second BigQuery schema
    :return: merged BigQuery schema
    """

    # Base case
    if len(schema2) == 0:
        # As non of the fields are present in the second schema
        # we need to make the ones in schema 1 nullable
        for field in schema1:
            # NULLABLE and REPEATED fields are fine,
            # so only adjust required ones
            if field['mode'] == MODE_REQUIRED:
                field['mode'] = MODE_NULLABLE
        return schema1

    # Inverse base case
    elif len(schema1) == 0:
        return merge_schema_fields_rec(schema2, schema1)

    # Recursive case
    else:

        merged_fields = []

        # Combine all fields that appear in both schemas
        # Note: field order does not matter
        for field1, field2 in get_matching_fields(schema1, schema2):
            merged_field = merge_record_schemas(field1, field2)
            merged_fields.append(merged_field)
            # field = merge_complex_schemas()

        # Nullify all fields that are in one side only
        # Note we abuse a recursive call here, as the base case deals with this situation
        unique_fields = list(get_nonmatching_fields(schema1, schema2))
        merged_unique_fields = merge_schema_fields_rec(unique_fields, [])

        return merged_fields + merged_unique_fields


def merge_record_schemas(field1: Dict[str, object], field2: Dict[str, object]) -> Dict[str, object]:
    # Merge and add complex types
    if field1['type'] == TYPE_RECORD and field2['type'] == TYPE_RECORD:
        return merge_record_field_schemas(field1, field2)

    # Merge and add primitive types
    elif field1['type'] != TYPE_RECORD and field2['type'] != TYPE_RECORD:
        return merge_primitive_schemas(field1, field2)

    # Otherwise raise an exception
    else:
        raise BQSchemaMergeException(f'Cannot merge record and primitive types')


def merge_record_field_schemas(field1: Dict[str, object], field2: Dict[str, object]) -> Dict[str, object]:
    # Extract the fields from both field schemas
    field1_schema = field1['fields']
    field2_schema = field2['fields']

    # Merge the fields and replace old version
    merged_subfields = merge_schema_fields_rec(field1_schema, field2_schema)
    result = dict(field1)
    result['fields'] = merged_subfields

    return result


def merge_primitive_schemas(schema1: Dict[str, object], schema2: Dict[str, object]) -> Dict[str, object]:
    """
    Merges two primitive BigQuery field schemas.
    We assume the name of both fields is equal.
    Type and mode are converted to a common one, if possible.
    If conversion is not possible, a BQSchemaMergeException is raised.

    :param schema1: schema entry of first primitive field
    :param schema2: schema entry of second primitive field
    :return: the merged schema
    """

    if schema1['name'] != schema2['name']:
        raise BQSchemaMergeException(f'Cannot merge fields with different names!')

    field_name = schema1['name']
    common_type = determine_common_type(schema1['type'], schema2['type'])
    common_mode = determine_common_mode(schema1['mode'], schema2['mode'])

    return {
        'name': field_name,
        'type': common_type,
        'mode': common_mode,
    }

def get_matching_fields(schema1: List[Dict], schema2: List[Dict]) -> List[Dict]:
    """
    Yields all top-level fields that appear in both schemas.

    :param schema1: first BigQuery schema
    :param schema2: second BigQuery schema
    :return: fields that appear in both schemas (top-level)
    """
    for field1 in schema1:
        for field2 in schema2:
            if field1['name'] == field2['name']:
                yield (field1, field2)


def get_nonmatching_fields(schema1: List[Dict], schema2: List[Dict]) -> List[Dict]:
    """
    Yields all top-level fields that appear in only one schema.

    :param schema1: first BigQuery schema
    :param schema2: second BigQuery schema
    :return: fields that appear in one of the schemas (top-level)
    """

    # Determine sets of unique top-level field names for both schemas
    fieldnames1 = {field['name'] for field in schema1}
    fieldnames2 = {field['name'] for field in schema2}

    # Yield unique fields on the left
    for field in schema1:
        if field['name'] not in fieldnames2:
            yield field

    # Yield unique fields on the right
    for field in schema2:
        if field['name'] not in fieldnames1:
            yield field




TYPE_MAP = {
    frozenset([TYPE_INTEGER, TYPE_FLOAT]): TYPE_FLOAT
}


def determine_common_type(type1: str, type2: str) -> str:
    """
    Based on type coercion doc on https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules#casting
    :param type1: type of first value
    :param type2: type of second value
    :return: common type, if available
    """
    try:
        type_set = frozenset([type1, type2])
        # If there is only one element in the set, they must be equal
        if len(type_set) == 1:
            return type1  # or: next(iter(mode_set))
        return TYPE_MAP[frozenset([type1, type2])]
    except KeyError:
        raise BQSchemaMergeException(f'Incompatible types encountered while merging: {type1}, {type2}')


MODE_MAP = {
    frozenset([MODE_NULLABLE, MODE_REQUIRED]): MODE_NULLABLE
}


def determine_common_mode(mode1: str, mode2: str) -> str:
    """
    Mode info can be found on https://cloud.google.com/bigquery/docs/schemas#modes
    :param mode1: type of first value
    :param mode2: type of second value
    :return: common mode, if available
    """
    try:
        mode_set = frozenset([mode1, mode2])
        # If there is only one element in the set, they must be equal
        if len(mode_set) == 1:
            return mode1  # or: next(iter(mode_set))
        return MODE_MAP[mode_set]
    except KeyError:
        raise BQSchemaMergeException(f'Incompatible modes encountered while merging: {mode1}, {mode2}')

