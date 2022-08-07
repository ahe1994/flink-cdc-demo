package com.ahery;

import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author ahery
 * @Created at 2022/8/6 9:08
 */
public class RcxWaitSource extends WaitSource {

  public static CopyOnWriteArrayList<String> data = new CopyOnWriteArrayList<>();

  public RcxWaitSource() {
    super(data);
  }
}
