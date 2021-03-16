package com.yeyangshu.streaming.transformation;

import com.yeyangshu.streaming.WorldCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * WorldCount数据写到Redis里
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/16 22:48
 */
public class World2Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = environment.socketTextStream("node1", 8888);

        SingleOutputStreamOperator<WorldCount> outStream = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] split = input.split(" ");
                for (String s : split) {
                    collector.collect(s);
                }
            }
        }).map(new MapFunction<String, WorldCount>() {
            @Override
            public WorldCount map(String s) throws Exception {
                return new WorldCount(s, 1L);
            }
        }).keyBy("word").sum(1);

        outStream.map(new RichMapFunction<WorldCount, String>() {

            Jedis jedis = null;
            // 在subtask启动的时候，首先调用的方法
            @Override
            public void open(Configuration parameters) throws Exception {
                // 连接到Redis
                jedis = new Jedis("localhost", 6379);
                // 选择Redis第3号数据库
                jedis.select(3);
            }

            // 在subtask执行完毕后，调用的方法
            @Override
            public void close() throws Exception {
                // 关闭Redis连接
                jedis.close();
            }

            // 处理每一个元素的方法
            @Override
            public String map(WorldCount worldCount) throws Exception {
                // 把数据存储进Redis中
                jedis.set(worldCount.word, String.valueOf(worldCount.count));
                return worldCount.word;
            }
        }).print();

        environment.execute();
    }

}
