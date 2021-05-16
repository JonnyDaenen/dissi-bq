from unittest import TestCase

from schematools.schema_extraction import extract_schema


class TestSchemaExtractionPrimitive(TestCase):

    def test_extract_float(self):
        input_json = '{"floatfield": 12.5}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "floatfield", "type": "FLOAT", "mode": "REQUIRED"},
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_integer(self):
        input_json = '{"intfield": 12}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "intfield", "type": "INTEGER", "mode": "REQUIRED"},
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_string(self):
        input_json = '{"stringfield":"basic string"}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "stringfield", "type": "STRING", "mode": "REQUIRED"},
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_bool(self):
        input_json = '{"boolfield": true}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "boolfield", "type": "BOOLEAN", "mode": "REQUIRED"},
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_null(self):
        input_json = '{"nullfield": null}'

        schema = extract_schema(input_json)

        expected_result = [
        ]
        self.assertEqual(expected_result, schema)

    def test_extract_timestamp(self):
        input_json = '{"timestampfield":"2020-06-18T10:44:12"}'

        schema = extract_schema(input_json)

        expected_result = [
            {"name": "timestampfield", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ]
        self.assertEqual(expected_result, schema)
