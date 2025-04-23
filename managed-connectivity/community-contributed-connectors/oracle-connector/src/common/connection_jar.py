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

# Default assumes jar file is in root of the project
JAR_PATH = "."

# Returns jar path allowing override with --jar option
def getJarPath(config : dict[str:str]):
    jar_path = '' 
    user_jar = config.get('jar')
    if user_jar is not None:
        # if file path to jar provided then use, otherwise current path + jar name
        if (user_jar.startswith(".") or user_jar.startswith("/")):
                jar_path = user_jar
        else:
                jar_path = Path(JAR_PATH).joinpath(user_jar)
    else:
        jar_path = Path(JAR_PATH).joinpath(JDBC_JAR)
    
    return jar_path
