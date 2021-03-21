package com.yeyangshu.streaming.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 广播
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/21 23:10
 */
public class BroadcastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = environment.generateSequence(1, 10).setParallelism(2);

        stream.writeAsText("./data/stream").setParallelism(2);

        stream.broadcast().writeAsText("./data/stream").setParallelism(4);

        environment.execute();
    }
}
