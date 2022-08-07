package com.ahery;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

/**
 * @Author ahery
 * @Created at 2022/8/3 8:13
 */
public class BocSink extends RichSinkFunction<Document> {

  private static final long serialVersionUID = 1L;
  MongoClient mongoClient = null;

  @Override
  public void invoke(Document value, Context context) throws Exception {
    if (value == null) {
      return;
    }

    try {
      if (mongoClient != null) {
        mongoClient = MongoUtil.getConnect();
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection collection = db.getCollection("user-sink");
        collection.insertOne(value);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void open(Configuration parms) throws Exception {
    super.open(parms);
    mongoClient = MongoUtil.getConnect();
  }

  @Override
  public void close() {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }
}
