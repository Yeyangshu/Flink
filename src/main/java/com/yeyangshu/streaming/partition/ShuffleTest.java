package com.yeyangshu.streaming.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * shuffle随机分发
 *
 * 场景：增大分区、提高并行度，解决数据倾斜
 * DataStream → DataStream
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/18 22:25
 */
public class ShuffleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = environment.generateSequence(1, 10).setParallelism(1);

        System.out.println(stream.getParallelism());

        /**
         * ShufflePartitioner源码，随机产生
         * private Random random = new Random();
         *
         * public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
         *     return this.random.nextInt(this.numberOfChannels);
         * }
         */
        stream.shuffle().print();

        environment.execute();
    }
}
