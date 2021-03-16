package com.yeyangshu.streaming.datasource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * Kafka来源
 * https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html
 *
 * @author zhumingxing
 * @date 2021/3/11 15:32
 **/
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "node01:9092");
        properties.put("group.id", "group01");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);

        // 简单的SimpleStringSchema
        DataStreamSource<String> source = environment.addSource(new FlinkKafkaConsumer<>("ule_execute_log", new SimpleStringSchema(), properties));

        // 定义的KafkaDeserializationSchema
        DataStreamSource<String> source1 = environment.addSource(new FlinkKafkaConsumer<>("ule_execute_log", new KafkaDeserializationSchema<String>() {
            // 什么时候停止，停止的条件是什么
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            // 需要进行序列化的字节流
            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                String key = new String(consumerRecord.key(), "UTF-8");
                String value = new String(consumerRecord.value(), "UTF-8");
                return key;
            }

            // 指定一下返回的数据类型，Flink提供的类型
            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, properties));


        source.print();

        environment.execute();
    }
}
