package com.yeyangshu.streaming.transformation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * KeyBy分流算子
 *
 * @author zhumingxing
 * @date 2021/3/11 17:00
 **/
public class KeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = environment.fromCollection(Arrays.asList("hello", "world", "hello", "flink"));
        source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String input) throws Exception {
                return "hello";
            }
        }).print();

        environment.execute();
    }
}
