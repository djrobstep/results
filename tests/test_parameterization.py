"""
Tests for SQL parameterization security and correctness.
"""
import pytest
from results.psyco import reformat_bind_params
from results.paging import quoted


class TestReformatBindParams:
    def test_basic_param(self):
        assert reformat_bind_params("SELECT :foo") == "SELECT %(foo)s"

    def test_param_at_start_of_query(self):
        # A query cannot start with :param (not valid SQL), but the function
        # should handle it without silently dropping the param.
        result = reformat_bind_params(":foo")
        assert "%(foo)s" in result

    def test_double_colon_cast_untouched(self):
        q = "SELECT x::text FROM t"
        assert reformat_bind_params(q) == q

    def test_double_colon_cast_with_param(self):
        result = reformat_bind_params("SELECT :val::integer")
        assert "%(val)s" in result
        assert "::integer" in result

    def test_colon_in_single_quoted_string_untouched(self):
        q = "SELECT 'https://example.com' FROM t WHERE x = :val"
        result = reformat_bind_params(q)
        assert "'https://example.com'" in result
        assert "%(val)s" in result
        assert "%(example" not in result

    def test_colon_in_dollar_quoted_block_untouched(self):
        q = "SELECT $$http://example.com$$ WHERE x = :val"
        result = reformat_bind_params(q)
        assert "$$http://example.com$$" in result
        assert "%(val)s" in result

    def test_colon_in_double_quoted_identifier_untouched(self):
        # unlikely but the parser should handle it
        q = 'SELECT "col:name" WHERE x = :val'
        result = reformat_bind_params(q)
        assert '"col:name"' in result
        assert "%(val)s" in result

    def test_multiple_params(self):
        result = reformat_bind_params("SELECT :a, :b WHERE x = :c")
        assert "%(a)s" in result
        assert "%(b)s" in result
        assert "%(c)s" in result

    def test_escaped_single_quote_in_string(self):
        q = "SELECT 'it''s:fine' WHERE x = :val"
        result = reformat_bind_params(q)
        assert "'it''s:fine'" in result
        assert "%(val)s" in result

    def test_no_params_unchanged(self):
        q = "SELECT 1"
        assert reformat_bind_params(q) == q

    def test_dollar_quoted_with_tag(self):
        q = "$body$http://example.com$body$ WHERE x = :val"
        result = reformat_bind_params(q)
        assert "$body$http://example.com$body$" in result
        assert "%(val)s" in result

    def test_rewrite_false_leaves_colon_style(self):
        q = "SELECT :foo"
        assert reformat_bind_params(q, rewrite=False) == q

    def test_url_in_literal_with_multiple_params(self):
        q = "INSERT INTO t (url, name) VALUES ('https://x.com', :name)"
        result = reformat_bind_params(q)
        assert "'https://x.com'" in result
        assert "%(name)s" in result

    def test_array_slice_colon_untouched(self):
        # PostgreSQL array slice: arr[1:3]
        q = "SELECT arr[1:3] FROM t WHERE x = :val"
        result = reformat_bind_params(q)
        assert "arr[1:3]" in result
        assert "%(val)s" in result


class TestQuoted:
    def test_simple_column(self):
        assert quoted("id") == '"id"'

    def test_column_with_internal_double_quote(self):
        # Internal " must be escaped to "" to prevent SQL injection
        result = quoted('col"name')
        assert result == '"col""name"'
        # Verify it doesn't produce a raw unescaped quote
        assert '"col"name"' != result

    def test_column_with_spaces(self):
        assert quoted("my column") == '"my column"'

    def test_injection_attempt(self):
        malicious = 'id" DESC; DROP TABLE users; --'
        result = quoted(malicious)
        # The result must be safely quoted — the internal " becomes ""
        assert result == '"id"" DESC; DROP TABLE users; --"'
        # Critically: when psycopg sees this as an identifier it's harmless
        assert result.count('"') % 2 == 0  # balanced outer quotes + escaped internals
