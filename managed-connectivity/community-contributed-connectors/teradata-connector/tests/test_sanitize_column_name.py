# Copyright 2026 Teradata
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for column name sanitization.

The sanitize_column_name_udf in entry_builder.py only converts non-ASCII
characters to _u<codepoint>_ format. Special characters (!@#$ etc.) are kept
as-is since they are valid Teradata column names.
"""

from src.name_builder import encode_non_ascii as sanitize_column_name


class TestSanitizeColumnName:

    # --- ASCII names pass through unchanged ---

    def test_normal_name_unchanged(self):
        assert sanitize_column_name("order_id") == "order_id"

    def test_special_chars_preserved(self):
        """Special characters are valid Teradata column names — keep as-is."""
        assert sanitize_column_name("!@#$%^&*{}|,?:;~") == "!@#$%^&*{}|,?:;~"

    def test_comma_preserved(self):
        assert sanitize_column_name("c decimal (4,2)") == "c decimal (4,2)"

    def test_spaces_preserved(self):
        assert sanitize_column_name("  col name  ") == "  col name  "

    def test_empty_string_unchanged(self):
        assert sanitize_column_name("") == ""

    def test_none_returns_none(self):
        assert sanitize_column_name(None) is None

    # --- Non-ASCII converted to _u<codepoint>_ ---

    def test_chinese_characters(self):
        result = sanitize_column_name("测试列")
        assert result == "_u6D4B__u8BD5__u5217_"
        assert "测" not in result

    def test_japanese_characters(self):
        result = sanitize_column_name("にほんご")
        assert result == "_u306B__u307B__u3093__u3054_"

    def test_mixed_ascii_and_chinese(self):
        result = sanitize_column_name("col_测试")
        assert result == "col__u6D4B__u8BD5_"

    def test_accented_characters(self):
        result = sanitize_column_name("café")
        assert result == "caf_u00E9_"

    def test_single_non_ascii(self):
        result = sanitize_column_name("ü")
        assert result == "_u00FC_"

    def test_emoji(self):
        result = sanitize_column_name("col_🎉")
        assert result.startswith("col_")
        assert "🎉" not in result

    def test_mixed_special_and_non_ascii(self):
        """Special chars kept, non-ASCII encoded."""
        result = sanitize_column_name("!@#测试")
        assert result.startswith("!@#")
        assert "_u6D4B_" in result
