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
from src.common.util import isRunningInContainer

# Returns jar path, allowing override with --jar option
def getJarPath(config : dict[str:str], jars_to_include: [str]) -> str:

    base_jar_path = "" 
    user_jar = config.get('jar')
    output_jar_path = ''

    # jar directory path depending on whether local script or running in container
    if isRunningInContainer():
        base_jar_path = "/opt/spark/jars"
    else:
        base_jar_path = "."

    if user_jar is not None:
        # if file path to jar provided then use it, otherwise current path + jar name
        if (user_jar.startswith(".") or user_jar.startswith("/")):
                output_jar_path = user_jar
        else:
                output_jar_path = f"{Path(base_jar_path).joinpath(user_jar)}"
    else:
        # Build path for one or more jar files
        for jar in jars_to_include:
            if len(output_jar_path) > 0:
                output_jar_path = f"{output_jar_path},{Path(base_jar_path).joinpath(jar)}"
            else:
                output_jar_path = f"{Path(base_jar_path).joinpath(jar)}"

    return output_jar_path
