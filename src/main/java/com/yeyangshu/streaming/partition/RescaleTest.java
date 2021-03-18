package com.yeyangshu.streaming.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 场景：减少分区 防止发生大量的网络传输 不会发生全量的重分区
 * DataStream → DataStream
 * 通过轮询分区元素，将一个元素集合从上游分区发送给下游分区，发送单位是集合，而不是一个个元素
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/18 23:38
 */
public class RescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = environment.generateSequence(1, 10).setParallelism(2);

        stream.writeAsText("./data/stream").setParallelism(2);

        stream.rescale().writeAsText("./data/stream").setParallelism(4);

        environment.execute();
    }
}
