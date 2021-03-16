package com.yeyangshu.streaming.transformation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Reduce
 * 归并操作， 并且操作每处理一个元素总是创建一个新值。即第一个元素和第二个元素处理得到一个新的元素，新的元素再和第三个元素做处理
 *
 * @author zhumingxing
 * @date 2021/3/11 17:10
 **/
public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 生成1~100之间的数字
        DataStreamSource<Long> source = environment.generateSequence(1, 100).setParallelism(1);
        source.keyBy(new KeySelector<Long, Long>() {
            @Override
            public Long getKey(Long input) throws Exception {
                if (input > 50) {
                    return 2L;
                }
                return 1L;
            }
        }).reduce(new ReduceFunction<Long>() {
            @Override
            public Long reduce(Long input1, Long input2) throws Exception {
                return input1 + input2;
            }
        }).print();

        environment.execute();
    }
}
