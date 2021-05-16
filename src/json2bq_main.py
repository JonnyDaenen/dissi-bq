import argparse
import logging

from json2bq import pipeline

if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.info("Hello pipeline!")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_pattern",
        help="Cloud Storage input pattern gs://...",
    )
    parser.add_argument(
        "--bq_dataset",
        help="Pre-exising dataset in BQ in which table(s) will be loaded."
    )
    parser.add_argument(
        "--bq_table",
        help="Table in BQ to create/update (will be created if it doesn't exist)."
    )
    parser.add_argument(
        "--load_data",
        help="Load data into resulting table",
        type=bool,
        default=True
    )
    parser.add_argument(
        "--bq_temp_location",
        help="Temp location for BigQuery loading",
    )
    parser.add_argument(
        "--setup_script",
        help="Script that contains all dependencies of the python job."
    )
    known_args, pipeline_args = parser.parse_known_args()

    pipeline.run(
        known_args.input_pattern,
        known_args.bq_dataset,
        known_args.bq_table,
        known_args.load_data,
        known_args.setup_script,
        known_args.bq_temp_location,
        pipeline_args,
    )
