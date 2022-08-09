package com.ahery;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

/**
 * @Author ahery
 * @Created at 2022/8/7 15:44
 */
public class MongoSink extends RichSinkFunction<Document> {

  private String dbName;
  private String collectionName;
  private MongoClient mongoClient = null;

  public MongoSink(String dbName, String collectionName) {
    this.dbName = dbName;
    this.collectionName = collectionName;
  }

  @Override
  public void invoke(Document value, Context context) throws Exception {
    if (value == null) {
      return;
    }

    try {
      if (mongoClient != null) {
        mongoClient = MongoUtil.getConnect();
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection collection = db.getCollection(collectionName);

        BasicDBObject query = new BasicDBObject();
        query.put("index", value.getInteger("index"));
        FindIterable findIterable = collection.find(query);
        if (findIterable.cursor().hasNext()) {
          return;
        }

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
