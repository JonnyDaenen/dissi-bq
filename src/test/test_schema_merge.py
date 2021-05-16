from unittest import TestCase

from schematools.exceptions import BQSchemaMergeException
from schematools.schema_extraction import extract_schema
from schematools.schema_merge import merge_schemas


class TestSchemaExtractionPrimitive(TestCase):

    def test_given_data_merge_schemas_simple(self):
        input1 = '{"field1": 123 }'
        input2 = '{"field1": null }'

        schema1 = extract_schema(input1)
        schema2 = extract_schema(input2)

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "INTEGER", "mode": "NULLABLE"}
        ]
        self.assertEqual(expected_result, final_schema)

    def test_given_data_merge_schemas_array(self):
        input1 = '{"field1": [1,2,3] }'
        input2 = '{"field1": [] }'

        schema1 = extract_schema(input1)
        schema2 = extract_schema(input2)

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "INTEGER", "mode": "REPEATED"}
        ]
        self.assertEqual(expected_result, final_schema)

    def test_given_data_merge_schemas_nested_simple(self):
        input1 = '{"started":{"pid":45678}}'
        input2 = '{"logged_in":{"username":"foo"}}'

        schema1 = extract_schema(input1)
        schema2 = extract_schema(input2)

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {
                "name": "started", "type": "RECORD", "mode": "NULLABLE",
                "fields": [{"name": "pid", "type": "INTEGER", "mode": "REQUIRED"}]  # Different from assignment
            },
            {
                "name": "logged_in", "type": "RECORD", "mode": "NULLABLE",
                "fields": [{"name": "username", "type": "STRING", "mode": "REQUIRED"}]  # Different from assignment
            }
        ]
        self.assertEqual(expected_result, final_schema)

    def test_given_data_merge_schemas_nested_mixed(self):
        input1 = '{"ts":"2020-06-18T10:44:12","started":{"pid":45678}}'
        input2 = '{"ts":"2020-06-18T10:44:13","logged_in":{"username":"foo"}}'

        schema1 = extract_schema(input1)
        schema2 = extract_schema(input2)

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {
                "name": "started", "type": "RECORD", "mode": "NULLABLE",
                "fields": [{"name": "pid", "type": "INTEGER", "mode": "REQUIRED"}]  # Different from assignment
            },
            {
                "name": "logged_in", "type": "RECORD", "mode": "NULLABLE",
                "fields": [{"name": "username", "type": "STRING", "mode": "REQUIRED"}]  # Different from assignment
            }
        ]
        self.assertEqual(expected_result, final_schema)

    def test_given_data_merge_schemas_primitive_and_complex(self):
        input1 = '{"started":{"pid":45678}}'
        input2 = '{"started":true}'

        schema1 = extract_schema(input1)
        schema2 = extract_schema(input2)

        with self.assertRaises(BQSchemaMergeException):
            merge_schemas(schema1, schema2)

    def test_merge_colliding_primitive_float(self):

        schema1 = [{"name": "field1", "type": "INTEGER", "mode": "REQUIRED"}]
        schema2 = [{"name": "field1", "type": "FLOAT", "mode": "REQUIRED"}]

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "FLOAT", "mode": "REQUIRED"},
        ]
        self.assertEqual(expected_result, final_schema)


    def test_merge_noncolliding_primitive(self):

        schema1 = [{"name": "field1", "type": "INTEGER", "mode": "REQUIRED"}]
        schema2 = [{"name": "field2", "type": "STRING", "mode": "REQUIRED"}]

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "field2", "type": "STRING", "mode": "NULLABLE"},
        ]
        self.assertEqual(expected_result, final_schema)


    def test_merge_nullable_and_required_primitive(self):

        schema1 = [{"name": "field1", "type": "INTEGER", "mode": "REQUIRED"}]
        schema2 = [{"name": "field1", "type": "INTEGER", "mode": "NULLABLE"}]

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "INTEGER", "mode": "NULLABLE"},
        ]
        self.assertEqual(expected_result, final_schema)

    def test_merge_nullable_and_repeated_primitive(self):

        schema1 = [{"name": "field1", "type": "INTEGER", "mode": "REPEATED"}]
        schema2 = [{"name": "field1", "type": "INTEGER", "mode": "NULLABLE"}]

        with self.assertRaises(BQSchemaMergeException):
            merge_schemas(schema1, schema2)


    def test_merge_required_and_missing_primitive(self):

        schema1 = [{"name": "field1", "type": "INTEGER", "mode": "REQUIRED"}]
        schema2 = []

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "INTEGER", "mode": "NULLABLE"},
        ]
        self.assertEqual(expected_result, final_schema)


    def test_merge_repeated_and_missing_primitive(self):

        schema1 = [{"name": "field1", "type": "INTEGER", "mode": "REPEATED"}]
        schema2 = []

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "INTEGER", "mode": "REPEATED"},
        ]
        self.assertEqual(expected_result, final_schema)


    def test_add_required_nested_field(self):

        schema1 = [{"name": "field1", "type": "RECORD", "mode": "NULLABLE", "fields": [
            {"name": "nested_field", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "nested_field2", "type": "INTEGER", "mode": "REQUIRED"}
        ]}]
        schema2 = [{"name": "field1", "type": "RECORD", "mode": "NULLABLE", "fields": [
            {"name": "nested_field", "type": "INTEGER", "mode": "REQUIRED"}
        ]}]

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "RECORD", "mode": "NULLABLE", "fields": [
                {"name": "nested_field", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "nested_field2", "type": "INTEGER", "mode": "NULLABLE"},
            ]},
        ]
        self.assertEqual(expected_result, final_schema)

    def test_merge_none_schema(self):

        schema1 = [{"name": "field1", "type": "INTEGER", "mode": "REQUIRED"}]
        schema2 = None

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "INTEGER", "mode": "REQUIRED"},
        ]
        self.assertEqual(expected_result, final_schema)

    def test_merge_none_schema_inverse(self):

        schema1 = None
        schema2 = [{"name": "field1", "type": "INTEGER", "mode": "REQUIRED"}]

        final_schema = merge_schemas(schema1, schema2)

        expected_result = [
            {"name": "field1", "type": "INTEGER", "mode": "REQUIRED"},
        ]
        self.assertEqual(expected_result, final_schema)