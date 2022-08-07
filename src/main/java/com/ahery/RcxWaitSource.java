package com.ahery;

import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author ahery
 * @Created at 2022/8/6 9:08
 */
public class WaitSource implements SourceFunction<String> {

  private boolean isRunning = true;
  public static CopyOnWriteArrayList<String> data = new CopyOnWriteArrayList<>();

  @Override
  public void run(SourceContext<String> sourceContext) throws Exception {
    while (isRunning) {
      while (!data.isEmpty()) {
        String msg = data.remove(0);
        sourceContext.collect(msg);
      }

      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  public void send(String s) {
    data.add(s);
  }
}
