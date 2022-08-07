package com.ahery;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.bson.Document;

/**
 * @Author ahery
 * @Created at 2022/8/6 9:02
 */
public class BocTask implements Task<String>, MapFunction<String, Document> {

  public static Map<String, Integer> bocMap = new ConcurrentHashMap<>();
  private final int TARGET_INDEX = 5;

  @Override
  public void execute(DataStreamSource source) {
    SingleOutputStreamOperator map = source.map(this);
    map.addSink(new BocSink());
  }

  @Override
  public Document map(String s) throws Exception {

    JSONObject json = JSONObject.parseObject(s);
    String docStr = json.getString("fullDocument");
    JSONObject doc = JSON.parseObject(docStr);

    Integer id = doc.getInteger("id");
    Integer index = doc.getInteger("index");
    String name = doc.getString("name");
    Integer age = doc.getInteger("age");

    String targetBocMapKey = id + "-" + TARGET_INDEX;
    if (index == TARGET_INDEX) {
      bocMap.put(targetBocMapKey, age);
    }

    Integer targetAge = bocMap.get(targetBocMapKey);
    if (targetAge == null) {
      BocWaitSource.data.add(s);
      return null;
    }

    Document document = new Document();
    document.append("id", id);
    document.append("index", index);
    document.append("name", name);
    document.append("diffAge", age - targetAge);

    return document;
  }
}
