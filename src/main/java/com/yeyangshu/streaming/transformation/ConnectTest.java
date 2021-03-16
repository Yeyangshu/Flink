package com.yeyangshu.streaming.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * connect算子，将两个数据流合并（假合并）
 * 优点：合并的这两个数据流中的元素可以不一样；union：必须一样
 *
 * @author zhumingxing
 * @date 2021/3/15 21:38
 **/
public class ConnectTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket1 = environment.socketTextStream("localhost", 8000);
        DataStreamSource<String> socket2 = environment.socketTextStream("localhost", 9000);

    }
}
