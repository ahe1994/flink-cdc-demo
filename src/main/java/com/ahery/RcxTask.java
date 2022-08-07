package com.ahery;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.bson.Document;

/**
 * @Author ahery
 * @Created at 2022/8/6 9:02
 */
public class RcxTask implements Task<String>, MapFunction<String, Document> {

  public static Map<Integer, ConcurrentHashMap<Integer, DataPoint>> rcxMap = new ConcurrentHashMap<>();

  @Data
  class DataPoint {

    Integer index;
    Integer age;
    Boolean preDep;
    Boolean nextDep;
  }

  @Override
  public void execute(DataStreamSource source) {
    SingleOutputStreamOperator map = source.map(this);
    map.addSink(new RcxSink());
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

    ConcurrentHashMap<Integer, DataPoint> dpMap = rcxMap.get(id);
    if (dpMap == null) {
      dpMap = new ConcurrentHashMap<>();
      rcxMap.put(id, dpMap);
    }

    DataPoint curDp = dpMap.get(index);
    if (curDp == null) {
      curDp = new DataPoint();
      curDp.index = index;
      curDp.age = age;
      curDp.preDep = index == 1 ? false : true;
      curDp.nextDep = true;
      dpMap.put(index, curDp);
    }

    if (!curDp.preDep) {
      return null;
    }

    int preIndex = index - 1;
    DataPoint preDp = dpMap.get(preIndex);
    if (preDp == null) {
      RcxWaitSource.data.add(s);
      return null;
    }

    int diffAge = curDp.age - preDp.age;
    Document document = new Document();
    document.append("id", id);
    document.append("index", index);
    document.append("name", name);
    document.append("diffAge", diffAge);

    curDp.preDep = false;
    preDp.nextDep = false;

    if (!curDp.nextDep) {
      dpMap.remove(curDp.index);
    }
    if (!preDp.preDep) {
      dpMap.remove(preDp.index);
    }

    return document;
  }
}
