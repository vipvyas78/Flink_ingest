# Flink Ingest

## Description

Small POC to ingest from a local Kafka broker and create a Data Stream using Flink which submits to the console

## Setup

### Python

  #### External packages required

  &nbsp;&nbsp;&nbsp;&nbsp;apache-flink

  &nbsp;&nbsp;&nbsp;&nbsp;kafka-python

  #### External jars

  The DataStreams use SQL connectors. The following jars should be  downloaded 

  [SQL Connector] (https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.12.0/flink-sql-connector-kafka_2.11-1.12.0.jar)

  Change the following lines in page_count_consumer.py & user_page_count_consumer.py  to addthe jar to runtime

  ``` python

  config.set_string("pipeline.jars",
                            "file:///your-absolue_file_location")

```

### Kafka

The POC has hardcoded literals for Kafka broker  hosts, ports and topic names and is currently configured to run on a broker running on the localhost

Hostname: localhost
Port : 9092

#### Topic
  The POC  ingests data from topic called SourceEvents
  
  
  
## Running the  POC

###Consumers

#### page_count_consumer.py

This program creates a datastream and connect to the EventSource topic as source. 
It then consumes data from Kafka and if the event_timestamp is in the last 7 days then it calculates the count grouped by page_id  and prints to standard output .

Everytime you  push messages to the topic the consumer will send the  updated count to the output

#### user_page_count_consumer.py

This program creates a datastream and connect to the EventSource topic as source. 
It then consumes data from Kafka and if the event_timestamp is in the last 7 days then it calculates the count grouped by user_id, page_id  and prints to standard output 
Everytime you  push messages to the topic the consumer will send the  updated count to the output



The above two consumers can be started first and they will wait for messages produced to kafka topic

``` python
python page_count_consumer.py
python user_page_count_consumer.py
```
### Producer

#### main.py

This program generates a list of 300 random pages in uuid4 format and 100 users.
300 events are produced into the target topic.
Timestamp of the events is randomly generated  and can be any of the last 10 days



```python
python main.py
```


## Logging

Logs will be created in  logs/app.log
If you want to change handlers and formatters,, then please do so in conf/logging.conf file


