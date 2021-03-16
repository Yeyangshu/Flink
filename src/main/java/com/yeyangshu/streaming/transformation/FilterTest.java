package com.yeyangshu.streaming.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Filter测试
 * 对每个元素都进行判断，返回为 true 的元素，如果为 false 则丢弃数据
 *
 * @author zhumingxing
 * @date 2021/3/11 16:56
 **/
public class FilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 生成1~100之间的数字
        DataStreamSource<Long> source = environment.generateSequence(1, 100).setParallelism(1);
        source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long input) throws Exception {
                if (input > 50) {
                    return true;
                }
                return false;
            }
        }).print();

        environment.execute();
    }
}
