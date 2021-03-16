package com.yeyangshu.streaming.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap测试
 * FlatMap可以代替filter
 * FlatMap算子：map flat(扁平化) flatMap返回集合
 *
 * @author zhumingxing
 * @date 2021/3/11 16:36
 **/
public class FlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 生成1~100之间的数字
        DataStreamSource<Long> source = environment.generateSequence(1, 100).setParallelism(1);
        source.flatMap(new FlatMapFunction<Long, Long>() {
            @Override
            public void flatMap(Long input, Collector<Long> collector) throws Exception {
                if (input % 2 == 0) {
                    collector.collect(input);
                }
            }
        }).flatMap(new MyFlatMapFunction()).print();

        environment.execute();
    }
}

class MyFlatMapFunction implements FlatMapFunction<Long, String> {

    @Override
    public void flatMap(Long input, Collector<String> collector) throws Exception {
        if (input >= 40 && input <= 50) {
            collector.collect(input.toString());
        }
    }
}
