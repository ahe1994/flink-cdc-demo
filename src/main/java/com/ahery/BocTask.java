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
public class Task1 implements Task<String>, MapFunction<String, Document> {

  public static Map<Integer, Integer> ageMap = new ConcurrentHashMap<>();

  @Override
  public void execute(DataStreamSource source) {
    SingleOutputStreamOperator map = source.map(this);
    map.addSink(new MongoSink());
  }

  @Override
  public Document map(String s) throws Exception {

    Main.log.info("Map source:***************{}" + s);
    JSONObject json = JSONObject.parseObject(s);
    String docStr = json.getString("fullDocument");
    JSONObject doc = JSON.parseObject(docStr);

    Integer id = doc.getInteger("id");
    String name = doc.getString("name");
    Integer age = doc.getInteger("age");

    if (id == 5) {
      ageMap.put(id, age);
    }

    Integer targetAge = ageMap.get(5);
    if (targetAge == null) {
      WaitSource.data.add(s);
      return null;
    }

    Document document = new Document();
    document.append("id", id);
    document.append("name", name);
    document.append("diffAge", age - targetAge);

    return document;
  }
}
