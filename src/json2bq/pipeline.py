from __future__ import absolute_import

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions

from json2bq.components import *
from json2bq.schema_accumulator import SchemaCombinerFn

logger = logging.getLogger()


def run(input_pattern, bq_dataset, bq_table, load_data=True, setup_script=None, temp_bq_location=None, pipeline_args=None):
    """
    Executes a JSON to BigQuery schema detection and optional data load.

    JSON schema is inferred from the input documents, which are provided in a JSONL format.

    The final schema is obtained by merging fields and relaxing required fields to nullable.
    Once the schema is calculated in a distributed fashion, a table is created or updated.
    In case the table is updated, the original table schema is merged with the new one.

    As a final optional step, the source data is loaded in the BigQuery table.

    :param input_pattern: the file pattern to read from
    :param bq_dataset: BigQuery dataset
    :param bq_table: BigQuery table
    :param load_data: load data into table
    :param temp_bq_location: temp GCS location to be used when loading data in BigQuery
    :param setup_script: setup script
    :param pipeline_args: general pipeline arguments
    """

    # `save_main_session` is set to true because some DoFn's rely on
    # globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args,
        save_main_session=True,
    )

    project_id = pipeline_options.view_as(GoogleCloudOptions).project

    # Set setup script so we include the right modules
    if setup_script:
        pipeline_options.view_as(SetupOptions).setup_file = setup_script

    with beam.Pipeline(options=pipeline_options) as pipeline:

        # Read input
        input_data = (pipeline
                 | "Read JSONLine Messages" >> ReadFromText(input_pattern)
                 )

        # Create/update BQ table schema
        bq_schema = (input_data
                     | "Extract Schemas" >> beam.Map(map_extract_schema)
                     | "Combine into 1 schema" >> beam.CombineGlobally(SchemaCombinerFn())
                     | "Create or update table" >> beam.Map(create_bq_table, project=project_id, dataset=bq_dataset, table=bq_table)
                     )

        # Branch to print the schema
        print_output = (bq_schema | "Print resulting schema" >> beam.Map(print_schema))

        # FUTURE write to bucket
        # bucket_output = ...

        # Branch to write the data to BigQuery, after the table has been created
        if load_data:
            bq_insert_result = (input_data
                      | "Parse json" >> beam.Map(json.loads)
                      | "Load data" >> beam.io.WriteToBigQuery(
                            table=bq_table,
                            dataset=bq_dataset,
                            project=project_id,
                            schema=prep_schema,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                            schema_side_inputs=(beam.pvalue.AsSingleton(bq_schema),),  # inject the calculated schema as a side input
                            custom_gcs_temp_location=temp_bq_location
                        )
                      )
