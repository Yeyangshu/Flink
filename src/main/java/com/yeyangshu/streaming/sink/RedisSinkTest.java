package com.yeyangshu.streaming.sink;

import com.yeyangshu.streaming.WorldCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * 将WorldCount结果写入Redis中
 * 注意：Flink是一个流式框架，Redis存数据时需要幂等操作
 * hello 1 hello 2
 *
 * hset(word, int)
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/31 23:58
 */
public class RedisSinkTest {
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

        // Redis单机：FlinkJedisPoolConfig
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setDatabase(3)
                .setHost("localhost")
                .setPort(6379)
                .build();
        // Redis集群：FlinkJedisClusterConfig
        // Redis哨兵：FlinkJedisSentinelConfig

        result.addSink(new RedisSink<>(redisConfig, new RedisMapper<WorldCount>() {
            // 指定操作Redis的命令
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "worldCount");
            }

            // key
            @Override
            public String getKeyFromData(WorldCount worldCount) {
                return worldCount.word;
            }

            // value
            @Override
            public String getValueFromData(WorldCount worldCount) {
                return String.valueOf(worldCount.count);
            }
        }));

        env.execute();

    }
}
