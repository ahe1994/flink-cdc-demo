package com.ahery;

import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author ahery
 * @Created at 2022/8/6 9:08
 */
public class BocWaitSource implements SourceFunction<String> {

  public static CopyOnWriteArrayList<String> data = new CopyOnWriteArrayList<>();
  private boolean isRunning = true;

  @Override
  public void run(SourceContext<String> sourceContext) throws Exception {
    while (isRunning) {
      Main.log.info("{}", BocWaitSource.data);
      while (!BocWaitSource.data.isEmpty()) {
        String msg = BocWaitSource.data.remove(0);
        sourceContext.collect(msg);
      }

      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
