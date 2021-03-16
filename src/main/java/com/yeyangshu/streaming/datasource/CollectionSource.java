package com.yeyangshu.streaming.datasource;

import com.yeyangshu.streaming.WorldCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 本地集合数据源
 *
 * @author zhumingxing
 * @date 2021/3/11 11:08
 **/
public class CollectionSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.fromCollection(Arrays.asList("hello world", "hello flink"));
        source.flatMap(new FlatMapFunction<String, WorldCount>() {
            @Override
            public void flatMap(String value, Collector<WorldCount> collector) throws Exception {
                String[] splits = value.split(" ");
                for (String world: splits) {
                    collector.collect(new WorldCount(world, 1L));
                }
            }
        }).keyBy("word").sum("count").print();
        environment.execute();
    }
}


