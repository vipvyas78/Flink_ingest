from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes, EnvironmentSettings, TableSink
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka, Elasticsearch
import logging
import logging.config


def page_aggs():
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        env_settings = EnvironmentSettings.Builder().use_blink_planner().build()
        t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)

        config = t_env.get_config().get_configuration()
        config.set_string("pipeline.jars",
                          "file:///home/vipul/PycharmProjects/Flink_ingest/venv/lib/flink-sql-connector-kafka_2.11-1"
                          ".12.0.jar")
        # specify connector and format jars

        eventsource_ddl = """
                CREATE TABLE events_table(
                    user_id VARCHAR,
                    page_id VARCHAR,
                    visit_timestamp VARCHAR
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'SourceEvents',
                  'properties.bootstrap.servers' = 'localhost:9092',
                  'properties.group.id' = 'consumer-flink',              
                  'format' = 'json'
                )
                """

        sink_page_count_ddl = f"""
            create table sink_page_count(
                C1 VARCHAR,
                page_id VARCHAR,
                C2 VARCHAR,
                PAGE_COUNT BIGINT
            ) WITH (
                'connector' = 'print'                     
              )
            """

        t_env.execute_sql(eventsource_ddl)
        t_env.execute_sql(sink_page_count_ddl)

        t_env.sql_query(
            "SELECT 'PAGE_ID -->' , page_id, 'COUNT-->', COUNT(*) AS PAGE_COUNT \
                 FROM events_table \
                 WHERE  TO_TIMESTAMP(visit_timestamp,'yyyy-MM-dd HH:mm:ss') >= TIMESTAMPADD(DAY,-7, CURRENT_TIMESTAMP) \
                 GROUP BY page_id ").execute_insert("sink_page_count").wait()
    except Exception as inst:
        logging.error(type(inst))
        logging.error('ERROR in DATA STREAM')
        logging.error(inst)


if __name__ == '__main__':
    logging.config.fileConfig('conf/logging.conf')
    page_aggs()
