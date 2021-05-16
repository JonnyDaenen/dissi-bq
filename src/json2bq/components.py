import io
import json
import logging

from apache_beam.typehints import Dict, List
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from schematools.schema_extraction import extract_schema
from schematools.schema_merge import merge_schemas

logger = logging.getLogger()


def dummy(x: object):
    """
    Dummy logging function, for debugging purposes.
    Simply logs a data point and emits it again.

    :param x: a data element
    :return: the input data element
    """

    logger.info(f'Just saw an event: {x}!')
    return x


def print_schema(schema: List[object]):
    logger.info(json.dumps(schema, indent=4))
    return schema


def map_extract_schema(data: Dict[str, object]) -> List[object]:
    """
    Mapper function to infer a schema from a given json document (1 line of JSONL file).
    :param data: JSON document, as a Python dict object
    :return: the inferred schema
    """

    schema = extract_schema(data)
    return schema


def create_bq_table(schema: List[object], project: str, dataset: str, table: str) -> List[object]:
    """
    Creates a table with a new schema, or updates the schema of an existing table.
    If the table already exists, a merge is performed with the new schema and the
    existing schema. The table is then updated accordingly.

    :param schema: schema of the table
    :param project: GCP project (must exist)
    :param dataset: Dataset in BigQuery (must exist)
    :param table: Target table
    :return: The final schema
    """

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # FUTURE check dataset

    # Determine if table already exists
    table_id = f'{project}.{dataset}.{table}'
    create = True
    try:
        client.get_table(table_id)
        logger.info(f'Table {table_id} already exists.')
        create = False
    except NotFound:
        logger.info(f'Table {table_id} not found.')

    # Create or Update table
    if create:
        inmem_input = io.StringIO(json.dumps(schema))
        final_schema = client.schema_from_json(inmem_input)

        table = bigquery.Table(table_id, schema=final_schema)
        client.create_table(table)
        logger.info(f'Created table {table_id}')

        return schema
    else:
        # Update the table

        # Get schema from existing table
        existing_table = client.get_table(table_id)
        existing_schema = existing_table.schema

        # Convert to schema to dictionary
        inmem_output = io.StringIO()
        client.schema_to_json(existing_schema, inmem_output)
        existing_schema_str = inmem_output.getvalue()
        existing_schema_list = json.loads(existing_schema_str)
        logger.info(f'Existing schema {existing_schema_str}')

        # Log proposed schema
        schema_str = json.dumps(schema, indent=4)
        logger.info(f'Proposed schema {schema_str}')

        # Merge schemas
        final_schema_list = merge_schemas(existing_schema_list, schema)
        final_schema_str = json.dumps(final_schema_list, indent=4)

        inmem_input = io.StringIO(final_schema_str)
        final_schema = client.schema_from_json(inmem_input)
        logger.info(f'Merged schema {final_schema_str}')

        # Update existing table with new schema
        existing_table.schema = final_schema
        client.update_table(existing_table, ["schema"])  # Make an API request.

        logger.info("Existing table updated with new schema")

        return final_schema_list


def prep_schema(destination, json_schema: List[object]) -> Dict[str, object]:
    """
    Prepares the schema in a format that the WriteToBigQuery transform can handle.

    :param destination: destination table
    :param json_schema: schema for the table
    :return: wrapped schema, compatible with WriteToBigQuery
    """

    return {
        'fields': json_schema
    }
