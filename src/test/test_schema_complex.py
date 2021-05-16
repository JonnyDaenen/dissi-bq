from unittest import TestCase

from schematools.exceptions import BQSchemaMergeException
from schematools.schema_extraction import extract_schema


class TestSchemaExtractionComplex(TestCase):

    def test_extract_array_int(self):
        input_json = '{"intarray": [1,2,3,4]}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "intarray", "type": "INTEGER", "mode": "REPEATED"},
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_array_string(self):
        input_json = '{"intarray": ["a","b","c","d"]}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "intarray", "type": "STRING", "mode": "REPEATED"},
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_array_with_null(self):
        input_json = '{"nullarray": ["a","b", null, "c","d"]}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "nullarray", "type": "STRING", "mode": "REPEATED"},
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_array_with_only_null(self):
        input_json = '{"nullarray": [null]}'

        with self.assertRaises(BQSchemaMergeException):
            schema = extract_schema(input_json)

    def test_extract_array_mixed(self):
        input_json = '{"mixedarray": [1, "a", "b", 2]}'

        with self.assertRaises(BQSchemaMergeException):
            schema = extract_schema(input_json)

    def test_extract_array_empty(self):
        input_json = '{"emptyarray": []}'

        schema = extract_schema(input_json)

        expected_result = []
        self.assertEqual(expected_result, schema)

    def test_extract_array_complex(self):
        input_json = '{"complexarray": [{"a":1}, {"b":2}]}'

        schema = extract_schema(input_json)

        expected_result = [
            {
                "name": "complexarray", "type": "RECORD", "mode": "REPEATED",
                "fields": [
                    {"name": "a", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "b", "type": "INTEGER", "mode": "NULLABLE"}
                ]
            }
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_array_nested(self):
        input_json = '{"nestedarray": [[1,2,3], [4,5,6]] }'

        with self.assertRaises(BQSchemaMergeException):
            schema = extract_schema(input_json)


    def test_extract_schema_nested(self):
        input_json = '{"started":{"pid":45678}}'

        schema = extract_schema(input_json)

        expected_result = [
            {
                "name": "started", "type": "RECORD", "mode": "REQUIRED",
                "fields": [{"name": "pid", "type": "INTEGER", "mode": "REQUIRED"}]
            }
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_schema_from_example_file1(self):
        input_json = '{"ts":"2020-06-18T10:44:12","started":{"pid":45678}}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {
                "name": "started", "type": "RECORD", "mode": "REQUIRED",
                "fields": [{"name": "pid", "type": "INTEGER", "mode": "REQUIRED"}]
            }
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_schema_from_example_file2(self):
        input_json = '{"ts":"2020-06-18T10:44:13","logged_in":{"username":"foo"}}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {
                "name": "logged_in", "type": "RECORD", "mode": "REQUIRED",
                "fields": [{"name": "username", "type": "STRING", "mode": "REQUIRED"}]
            }
        ]
        self.assertEqual(expected_result, schema)
