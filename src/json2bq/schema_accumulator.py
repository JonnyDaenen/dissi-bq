import apache_beam as beam

from schematools.schema_merge import merge_schemas


class SchemaCombinerFn(beam.CombineFn):
    """
    Schema combiner.
    Merges an undefined number of schemas into a
    relaxed version, provided that they are compatible.
    """
    def create_accumulator(self):
        return None

    def add_input(self, accumulator, input):
        return merge_schemas(accumulator, input)

    def merge_accumulators(self, accumulators):
        merged = None
        for accum in accumulators:
            merged = merge_schemas(merged, accum)
        return merged

    def extract_output(self, accumulator):
        return accumulator
