package com.yeyangshu.streaming.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 上游分区与下游分区一一对应的分发
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/24 23:18
 */
public class ForwardTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = environment.generateSequence(1, 10).setParallelism(2);

        stream.writeAsText("./data/stream").setParallelism(2);

        stream.forward().writeAsText("./data/stream").setParallelism(2);

        environment.execute();
    }
}
