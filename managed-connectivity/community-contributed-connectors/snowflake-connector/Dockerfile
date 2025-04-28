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

FROM debian:11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y procps tini
RUN apt install -y wget

ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/
RUN mkdir -p "${SPARK_EXTRA_JARS_DIR}"
COPY snowflake-jdbc-3.19.0.jar "${SPARK_EXTRA_JARS_DIR}"
COPY spark-snowflake_2.12-3.1.1.jar "${SPARK_EXTRA_JARS_DIR}"

ENV RUNNING_IN_CONTAINER=Y

ENV CONDA_HOME=/opt/miniconda3
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python
ENV PATH=${CONDA_HOME}/bin:${PATH}
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-py311_24.9.2-0-Linux-x86_64.sh

RUN bash Miniconda3-py311_24.9.2-0-Linux-x86_64.sh -b -p /opt/miniconda3 \
  && ${CONDA_HOME}/bin/conda config --system --set always_yes True \
  && ${CONDA_HOME}/bin/conda config --system --set auto_update_conda False \
  && ${CONDA_HOME}/bin/conda config --system --prepend channels conda-forge \
  && ${CONDA_HOME}/bin/conda config --system --set channel_priority strict

RUN ${CONDA_HOME}/bin/conda install mamba -n base -c conda-forge \
    && ${CONDA_HOME}/bin/mamba install \
      conda \
      google-cloud-dataproc \
      google-cloud-logging \
      google-cloud-monitoring \
      google-cloud-storage

RUN apt update && apt install -y git
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

ENV PYTHONPATH=/opt/python/packages
RUN mkdir -p "${PYTHONPATH}/src/"
COPY src/ "${PYTHONPATH}/src/"
COPY main.py .

RUN groupadd -g 1099 spark
RUN useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark