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

# Jar files and paths for when connector is containerised

from pathlib import Path
from src.constants import JDBC_JAR

# Standard location for spark jars
JAR_PATH = "/opt/spark/jars"

def getJarPath(config : dict[str:str]) -> str:
    jar_path = '' 
    user_jar = config.get('jar')
    if user_jar is not None:  
        jar_path = Path(JAR_PATH).joinpath(user_jar)
    else:
        jar_path = Path(JAR_PATH).joinpath(JDBC_JAR)
    
    return jar_path
