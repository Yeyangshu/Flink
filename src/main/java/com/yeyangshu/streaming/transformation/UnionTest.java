package com.yeyangshu.streaming.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 将两个数据流进行合并
 * 条件：数据流中的元素必须一致
 *
 * @author zhumingxing
 * @date 2021/3/14 10:25
 **/
public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source1 = environment.fromCollection(Arrays.asList(1, 2, 3));
        DataStreamSource<Integer> source2 = environment.fromCollection(Arrays.asList(4, 5, 6));

        DataStream<Integer> union = source1.union(source2);
        union.print();

        environment.execute();

    }
}
