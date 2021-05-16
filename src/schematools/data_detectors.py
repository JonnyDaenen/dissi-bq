

def is_timestamp(field_Value: str) -> bool:
    """
    Checks if a string is compatible with the TIMESTAMP
    type of BigQuery.

    Doc: https://github.com/googleapis/python-bigquery/blob/8bcf397fbe2527e06317741875a059b109cfcd9c/tests/unit/test__helpers.py#L218

    :param field_Value: a field value (string)
    :return: whether the value is TIMESTAMP compatible
    """
    from google.cloud.bigquery._helpers import _timestamp_query_param_from_json
    try:
        result = _timestamp_query_param_from_json(field_Value, {'mode': 'NULLABLE', 'name':'unknown', 'field_type':'UNKNOWN'})
        return result is not None
    except:
        return False


def is_date(field_Value: str) -> bool:
    # FUTURE implement
    return False


def is_time(field_Value: str) -> bool:
    # FUTURE implement
    return False



