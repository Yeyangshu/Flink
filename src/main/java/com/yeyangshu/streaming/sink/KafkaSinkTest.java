package com.yeyangshu.streaming.sink;

import com.yeyangshu.streaming.WorldCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.Nullable;
import java.util.Properties;


/**
 * Flink默认支持将处理结果写入到kafka topic中
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/4/6 22:19
 */
public class KafkaSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<WorldCount> result = stream
                .flatMap(new FlatMapFunction<String, WorldCount>() {
                    @Override
                    public void flatMap(String input, Collector<WorldCount> collector) throws Exception {
                        String[] split = input.split(" ");
                        collector.collect(new WorldCount(split[0], Long.valueOf(split[1])));
                    }
                })
                .keyBy(1)
                .sum(1);

        // Kafka属性设置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);



        result.addSink(new FlinkKafkaProducer("wordCount", new KafkaSerializationSchema<>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Object o, @Nullable Long aLong) {
                        return null;
                    }
                }),
                properties,
                org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        env.execute();
    }
}
