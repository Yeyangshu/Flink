package com.yeyangshu.streaming.datasource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.collection.Iterator;
import scala.io.Source;

import java.util.Properties;

/**
 * Kafka发送数据
 *
 * @author zhumingxing
 * @date 2021/3/14 15:43
 **/
public class FlinkKafkaProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("group.id", "group");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Iterator<String> iterator = Source.fromFile("/Users/zhumingxing/Downloads/data_carFlow_all_column_test.txt", "UTF-8").getLines();
        while (iterator.hasNext()) {
            String line = iterator.next();
            String[] splits = line.split(",");
            String monitorId = splits[0].replace("'", "");
            String carId = splits[2].replace("'","");
            String timestamp = splits[4].replace("'","");
            String speed = splits[6];
            StringBuilder builder = new StringBuilder();
            String info = builder.append(monitorId + "\t").append(carId + "\t").append(timestamp + "\t").append(speed).toString();
            System.out.println("--------" + info);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", info);
            producer.send(record);
        }
    }
}
