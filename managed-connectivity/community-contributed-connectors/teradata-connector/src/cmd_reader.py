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

"""Reads command-line arguments for the Teradata connector."""

import argparse
import os
import re
import sys

from src.common.argument_validator import validateArguments, validateQueryBand, validateSecretID
from src.common.secret_manager import get_password


def read_args():
    parser = argparse.ArgumentParser()

    # Standard Dataplex target arguments
    parser.add_argument(
        "--target_project_id",
        type=str,
        required=True,
        help="Google Cloud Project ID for metadata import",
    )
    parser.add_argument(
        "--target_location_id",
        type=str,
        required=True,
        help="Google Cloud region for metadata import",
    )
    parser.add_argument(
        "--target_entry_group_id",
        type=str,
        required=True,
        help="Dataplex Entry Group ID",
    )

    # Teradata connection arguments
    parser.add_argument(
        "--host",
        type=str,
        required=True,
        help="Teradata server hostname",
    )
    parser.add_argument(
        "--port",
        type=int,
        required=False,
        default=1025,
        help="Teradata server port (default: 1025)",
    )
    parser.add_argument(
        "--user",
        type=str,
        required=False,
        default=None,
        help="Teradata username",
    )
    parser.add_argument(
        "--password_secret",
        type=str,
        required=False,
        help="Secret Manager ID for the Teradata password",
    )
    parser.add_argument(
        "--password",
        type=str,
        required=False,
        help=(
            "Teradata password (least secure; prefer "
            "--password_secret, --password_file, or "
            "TERADATA_PASSWORD)"
        ),
    )
    # Authentication mechanism
    parser.add_argument(
        "--logmech",
        type=str,
        required=False,
        default=None,
        help="Teradata logon mechanism: TD2, LDAP, JWT",
    )
    parser.add_argument(
        "--logdata",
        type=str,
        required=False,
        default=None,
        help=(
            "Additional logon data for the selected --logmech "
            "(for example, LDAP credentials or JWT tokens)"
        ),
    )
    parser.add_argument(
        "--logdata_secret",
        type=str,
        required=False,
        default=None,
        help=(
            "Secret Manager ID for logdata "
            "(projects/{PROJECT}/secrets/{SECRET})"
        ),
    )

    parser.add_argument(
        "--password_file",
        type=str,
        required=False,
        help=(
            "Path to file containing the Teradata password "
            "(alternative to --password_secret, "
            "TERADATA_PASSWORD, or --password)"
        ),
    )

    parser.add_argument(
        "--database",
        type=str,
        required=False,
        default=None,
        help="Scope extraction to a specific database (optional)",
    )

    # JDBC jar override
    parser.add_argument(
        "--jar",
        type=str,
        required=False,
        help="Path to JDBC jar file if using a different version",
    )

    # JDBC charset
    parser.add_argument(
        "--charset",
        type=str,
        required=False,
        default="UTF8",
        help="Teradata JDBC session character set (default: UTF8)",
    )

    # Optional query band for Teradata sessions
    parser.add_argument(
        "--query_band",
        type=str,
        required=False,
        help="Teradata query band (optional)",
    )

    # Output destination
    output_option_group = parser.add_mutually_exclusive_group()
    output_option_group.add_argument(
        "--local_output_only",
        action="store_true",
        help="Output metadata file locally only",
    )
    output_option_group.add_argument(
        "--output_bucket",
        type=str,
        help="GCS bucket for metadata output (no gs:// prefix)",
    )

    parser.add_argument(
        "--output_folder",
        type=str,
        required=False,
        help="Folder within bucket for output",
    )

    parser.add_argument(
        "--min_expected_entries",
        type=int,
        required=False,
        default=-1,
        help=(
            "Minimum entries expected; fewer = no upload to GCS"
        ),
    )

    parsed_args = parser.parse_known_args()[0]

    # Validate charset — must be alphanumeric/underscore only to prevent
    # injection of additional JDBC URL parameters via delimiters like , or /
    parsed_args.charset = parsed_args.charset.strip()
    if not re.match(r"^[A-Za-z0-9_]+$", parsed_args.charset):
        raise SystemExit(
            f"Error: invalid --charset value '{parsed_args.charset}'. "
            "Must contain only letters, numbers, and underscores."
        )

    # Validate and normalize query band (applies defaults if not provided)
    parsed_args.query_band = validateQueryBand(parsed_args.query_band)

    # Validate logmech value
    valid_logmechs = {"TD2", "LDAP", "JWT"}
    if parsed_args.logmech is not None:
        if parsed_args.logmech.upper() not in valid_logmechs:
            raise SystemExit(
                f"Error: invalid --logmech value "
                f"'{parsed_args.logmech}'. "
                f"Supported values: "
                f"{', '.join(sorted(valid_logmechs))}"
            )
        parsed_args.logmech = parsed_args.logmech.upper()

    # Resolve logdata from Secret Manager if needed
    if parsed_args.logdata is not None and parsed_args.logdata_secret is not None:
        raise SystemExit(
            "Error: --logdata and --logdata_secret "
            "are mutually exclusive"
        )
    if parsed_args.logdata_secret:
        validateSecretID(parsed_args.logdata_secret)
        parsed_args.logdata = get_password(parsed_args.logdata_secret)

    # Password resolution priority:
    # 1. --password_secret (Google Secret Manager)
    # 2. --password_file (local file)
    # 3. TERADATA_PASSWORD environment variable
    # 4. --password CLI argument (with security warning)
    password_resolved = False
    if parsed_args.password_secret:
        try:
            parsed_args = validateArguments(parsed_args)
        except Exception as exc:
            raise SystemExit(f"Error: {exc}")
        password_resolved = True
    elif parsed_args.password_file:
        try:
            with open(
                parsed_args.password_file, "r", encoding="utf-8"
            ) as f:
                password_from_file = f.read().strip()
        except FileNotFoundError:
            raise SystemExit(
                f"Error: password file not found: "
                f"{parsed_args.password_file}"
            )
        except UnicodeDecodeError:
            raise SystemExit(
                f"Error: password file contains invalid UTF-8: "
                f"{parsed_args.password_file}"
            )
        except OSError as exc:
            raise SystemExit(
                f"Error: unable to read password file "
                f"{parsed_args.password_file}: {exc}"
            )

        if not password_from_file:
            raise SystemExit(
                f"Error: password file is empty or contains only "
                f"whitespace: {parsed_args.password_file}"
            )

        parsed_args.password = password_from_file
        password_resolved = True
    elif "TERADATA_PASSWORD" in os.environ:
        password_from_env = os.environ["TERADATA_PASSWORD"].strip()
        if not password_from_env:
            raise SystemExit(
                "Error: TERADATA_PASSWORD is empty or contains "
                "only whitespace."
            )
        parsed_args.password = password_from_env
        password_resolved = True
    elif parsed_args.password is not None:
        parsed_args.password = parsed_args.password.strip()
        if not parsed_args.password:
            raise SystemExit(
                "Error: --password value is empty or contains "
                "only whitespace."
            )
        print(
            "WARNING: Using --password on the command line exposes "
            "credentials in process listings and shell history. "
            "Consider using --password_secret, --password_file, "
            "or the TERADATA_PASSWORD environment variable instead.",
            file=sys.stderr,
        )
        password_resolved = True

    # Determine if user/password are required based on logmech
    credentials_optional = {"LDAP", "JWT"}
    effective_logmech = parsed_args.logmech

    if effective_logmech in credentials_optional:
        # User/password not required — default to empty if omitted
        if not parsed_args.user:
            parsed_args.user = ""
        if not password_resolved:
            parsed_args.password = ""
    else:
        # TD2 (default): user + password required
        if not parsed_args.user:
            raise SystemExit(
                "Error: --user is required when --logmech is "
                f"'{effective_logmech or 'TD2'}'"
            )
        if not password_resolved:
            raise SystemExit(
                "Error: no password provided. Use one of the "
                "following methods:\n"
                "  --password_secret  Google Secret Manager "
                "(recommended for GCP)\n"
                "  --password_file    Path to a file containing "
                "the password\n"
                "  TERADATA_PASSWORD  Environment variable\n"
                "  --password         CLI argument (least secure)"
            )

    # Validate output args if not using Secret Manager path
    if not parsed_args.password_secret:
        if (
            not parsed_args.local_output_only
            and not parsed_args.output_bucket
        ):
            raise SystemExit(
                "Error: --output_bucket and --output_folder "
                "required if not using --local_output_only"
            )

    return vars(parsed_args)
