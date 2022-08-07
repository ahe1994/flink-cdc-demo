package com.ahery;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * @Author ahery
 * @Created at 2022/8/6 9:01
 */
public interface Task<T> {

  void execute(DataStreamSource<T> dataStreamSource);
}
