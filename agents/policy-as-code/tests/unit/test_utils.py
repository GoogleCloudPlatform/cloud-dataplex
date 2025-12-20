from policy_as_code_agent.llm_utils import get_json_schema_from_content


def test_get_json_schema_simple():
    content = '[{"name": "Alice", "age": 30}]'
    schema = get_json_schema_from_content(content)
    expected = {"name": "str", "age": "int"}
    assert schema == expected


def test_get_json_schema_nested():
    content = '[{"user": {"id": 1, "details": {"active": true}}}]'
    schema = get_json_schema_from_content(content)
    expected = {"user": {"id": "int", "details": {"active": "bool"}}}
    assert schema == expected


def test_get_json_schema_list():
    content = '[{"tags": ["a", "b"]}]'
    schema = get_json_schema_from_content(content)
    # The util merges list schemas, usually taking the first item's type
    expected = {"tags": ["str"]}
    assert schema == expected


def test_get_json_schema_jsonl():
    content = '{"a": 1}\n{"b": 2}'
    schema = get_json_schema_from_content(content)
    # It merges keys from both lines
    expected = {"a": "int", "b": "int"}
    assert schema == expected
