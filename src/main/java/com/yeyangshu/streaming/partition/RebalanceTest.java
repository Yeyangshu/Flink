package com.yeyangshu.streaming.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 轮询
 * 场景：增大分区、提高并行度，解决数据倾斜
 * DataStream → DataStream
 * 轮询分区元素，均匀的将元素分发到下游分区，下游每个分区的数据比较均匀，在发生数据倾斜时非常
 * 有用，网络开销比较大
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/18 22:34
 */
public class RebalanceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = environment.generateSequence(1, 100).setParallelism(3);

        System.out.println(stream.getParallelism());

        stream.rebalance().print();

        environment.execute();
    }
}
