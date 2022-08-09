package com.ahery;

/**
 * @Author ahery
 * @Created at 2022/8/3 8:13
 */
public class BocSink extends MongoSink {

  public BocSink() {
    super(Conf.instance().getSinkDatabase(), "user-boc");
  }
}
