# Jar files and paths for when connector is running as local script

JDBC_JAR_FILE = "snowflake-jdbc-3.19.0.jar"
SF_SPARK_JAR_FILE = "spark-snowflake_2.12-3.1.1.jar"

SPARK_JAR_PATH = f"{JDBC_JAR_FILE},{SF_SPARK_JAR_FILE}"