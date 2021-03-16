package com.yeyangshu.streaming.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Map练习
 *
 * @author zhumingxing
 * @date 2021/3/11 16:09
 **/
public class MapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 生成1~100之间的数字
        DataStreamSource<Long> source = environment.generateSequence(1, 100).setParallelism(1);
        // <Long, String>，左边输入类型，右边输出类型
        source.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long num) throws Exception {
                return num.toString();
            }
        }).map(new MyMapFunction()).print();

        environment.execute();
    }

}

/**
 * 自定义map
 */
class MyMapFunction implements MapFunction<String, Float> {
    @Override
    public Float map(String input) throws Exception {
        return Float.valueOf(input);
    }
}