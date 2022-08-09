package com.ahery;

import lombok.Data;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @Author ahery
 * @Created at 2022/8/9 7:37
 */
@Data
public class Conf {

  public static Conf conf;

  private String sourceHost = "localhost";
  private int sourcePort = 27017;
  private String sourceUser;
  private String sourcePassword;
  private String sourceDatabase;

  private String sinkHost = "localhost";
  private int sinkPort = 27017;
  private String sinkUser;
  private String sinkPassword;
  private String sinkDatabase;

  private Conf() {
  }

  public static synchronized Conf instance() {
    if (conf == null) {
      conf = new Conf();
    }

    return conf;
  }

  public synchronized void init(ParameterTool parameters) {
    conf = new Conf();
    conf.sourceHost = parameters.get("mongo.source.host");
    conf.sourcePort = parameters.getInt("mongo.source.port");
    conf.sourceUser = parameters.get("mongo.source.user");
    conf.sourcePassword = parameters.get("mongo.source.password");
    conf.sourceDatabase = parameters.get("mongo.source.database");

    conf.sinkHost = parameters.get("mongo.sink.host");
    conf.sinkPort = parameters.getInt("mongo.sink.port");
    conf.sinkUser = parameters.get("mongo.sink.user");
    conf.sinkPassword = parameters.get("mongo.sink.password");
    conf.sinkDatabase = parameters.get("mongo.sink.database");
  }
}
