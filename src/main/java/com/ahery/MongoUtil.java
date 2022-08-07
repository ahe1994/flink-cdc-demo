package com.ahery;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * @Author ahery
 * @Created at 2022/8/3 8:15
 */
public class MongoUtil {

  public static MongoClient getConnect() {
    String uri = "mongodb://root:123456@81.69.24.2:27017/?maxPoolSize=20&w=majority";
    try {
      return MongoClients.create(uri);
    } catch (Exception e) {
      return null;
    }
  }
}
