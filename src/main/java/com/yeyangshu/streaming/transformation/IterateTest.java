package com.yeyangshu.streaming.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/16 21:36
 */
public class IterateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = environment.socketTextStream("node1", 8888);

        IterativeStream<Integer> iterate = stream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String input) throws Exception {
                return Integer.valueOf(input);
            }
        }).iterate();

        SingleOutputStreamOperator<Integer> feedback = iterate.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer input) throws Exception {
                return input <= 0;
            }
        });

        feedback.print();

        iterate.closeWith(feedback);

        iterate.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer input) throws Exception {
                return input > 0;
            }
        });

        environment.execute();
    }
}
