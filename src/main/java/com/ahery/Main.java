package com.ahery;


import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author ahery
 * @Created at 2022/8/2 19:57
 */
public class Main {

  public static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {

    mongoCdc();
  }

  public static void mongoCdc() throws Exception {
    SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
        .hosts("81.69.24.2:27017")
        .username("root")
        .password("123456")
        .databaseList("test") // set captured database, support regex
        .collectionList("test.user") //set captured collections, support regex
        .deserializer(new JsonDebeziumDeserializationSchema())
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    DataStreamSource<String> streamSource = env.addSource(sourceFunction);
    new BocTask().execute(streamSource);
    new RcxTask().execute(streamSource);

    DataStreamSource<String> bocDataStream = env.addSource(new BocWaitSource());
    new BocTask().execute(bocDataStream);

    DataStreamSource rcxDataStream = env.addSource(new RcxWaitSource());
    new RcxTask().execute(rcxDataStream);

    env.execute();
  }
}
