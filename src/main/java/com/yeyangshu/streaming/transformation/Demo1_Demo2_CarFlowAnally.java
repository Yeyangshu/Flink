package com.yeyangshu.streaming.transformation;

import com.yeyangshu.streaming.WorldCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * Kafka来源
 * demo1：从Kafka中消费数据，统计各个卡口的流量
 * demo2：从Kafka中消费数据，统计每一分钟每一个卡口的流量，构建key，时间映射成分钟字段，然后world count
 *
 * @author zhumingxing
 * @date 2021/3/11 15:32
 **/
public class Demo1_Demo2_CarFlowAnally {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("group.id", "group");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);

        properties.put("auto.offset.reset", "latest");
        // 简单的SimpleStringSchema
        DataStreamSource<String> stream = environment.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));

        // 相同key的数据一定是由某一个subtask处理，一个subtask可能会处理多个key所对应的数据
        stream
                // 消息 -> WorldCount(monitorId, count)
                .map(new MapFunction<String, WorldCount>() {
                    @Override
                    public WorldCount map(String input) throws Exception {
                        String[] split = input.split("\t");
                        String monitorId = split[0];
                        String timestamp = split[2].substring(0, 16);
                        // 通过时间构建key，统计每分钟卡口流量
                        String key = monitorId + " " + timestamp;
                        //
                        return new WorldCount(key, 1L);
                    }
                })
                // 根据word分组
                .keyBy("word")
                // input1：上次聚合的结果；input2：本次要聚合的数据
                .reduce(new ReduceFunction<WorldCount>() {
                    @Override
                    public WorldCount reduce(WorldCount input1, WorldCount input2) throws Exception {
                        return new WorldCount(input1.word, input1.count + input2.count);
                    }
                })
                .print();

        environment.execute();
    }
}
