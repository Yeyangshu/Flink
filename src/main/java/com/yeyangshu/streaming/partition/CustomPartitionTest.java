package com.yeyangshu.streaming.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义分区
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/24 23:27
 */
public class CustomPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = environment.generateSequence(1, 10).setParallelism(2);

        stream.writeAsText("./data/stream").setParallelism(2);

        // 根据哪个字段来分区
        stream.partitionCustom(new CustomPartitioner(), 0).writeAsText("./data/stream").setParallelism(4);

        environment.execute();
    }
    static class CustomPartitioner implements Partitioner<Long> {
        @Override
        public int partition(Long key, int numPartitions) {
            return key.intValue() % numPartitions;
        }
    }
}
