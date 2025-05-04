# Copyright 2025 Google LLC
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

# Jar files and paths for when connector is running as local script

from pathlib import Path
from src.constants import JDBC_JAR
from src.constants import SNOWFLAKE_SPARK_JAR
from src.common.util import isRunningInContainer

# Returns jar path, allowing override with --jar option
def getJarPath(config : dict[str:str]):

    base_jar_path = "" 
    user_jar = config.get('jar')

    # jar directory path depending on whether local script or running in container
    if isRunningInContainer():
        base_jar_path = "/opt/spark/jars"
    else:
        base_jar_path = "."

    if user_jar is not None:
        # if file path to jar provided then use, otherwise current path + jar name
        if (user_jar.startswith(".") or user_jar.startswith("/")):
                jar_path = user_jar
        else:
                jar_path = Path(base_jar_path).joinpath(user_jar)
    else:
        jdbc_jar_path = Path(base_jar_path).joinpath(JDBC_JAR)
        sf_spark_jar_path = Path(base_jar_path).joinpath(SNOWFLAKE_SPARK_JAR)
        jar_path = f"{jdbc_jar_path},{sf_spark_jar_path}"

    return jar_path
