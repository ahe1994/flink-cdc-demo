package com.ahery;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * @Author ahery
 * @Created at 2022/8/3 8:15
 */
public class MongoUtil {

  public static MongoClient getConnect() {
    Conf conf = Conf.instance();
//    String uri = "mongodb://root:123456@81.69.24.2:27017/?maxPoolSize=20&w=majority";
    String uri = String.format("mongodb://%s:%s@%s:%d/?maxPoolSize=20&w=majority",
        conf.getSinkUser(),
        conf.getSinkPassword(),
        conf.getSinkHost(),
        conf.getSinkPort());
    try {
      return MongoClients.create(uri);
    } catch (Exception e) {
      return null;
    }
  }
}
