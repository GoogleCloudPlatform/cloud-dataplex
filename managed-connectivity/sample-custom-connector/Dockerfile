FROM debian:11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y procps tini
RUN apt install -y wget

ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/
RUN mkdir -p "${SPARK_EXTRA_JARS_DIR}"
COPY ojdbc11.jar "${SPARK_EXTRA_JARS_DIR}"

ENV CONDA_HOME=/opt/miniconda3
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python
ENV PATH=${CONDA_HOME}/bin:${PATH}
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-py311_24.9.2-0-Linux-x86_64.sh

RUN bash Miniconda3-py310_23.3.1-0-Linux-x86_64.sh -b -p /opt/miniconda3 \
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