package com.yeyangshu.streaming.transformation;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 根据某一条件拆分数据流
 *
 * @author zhumingxing
 * @date 2021/3/14 10:45
 **/
public class SplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 需求，偶数分到一个流中，奇数分到另一个流中
        DataStreamSource<Long> source = environment.generateSequence(1, 100);

        SplitStream<Long> splitStream = source.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long input) {
                ArrayList<String> outList = new ArrayList<>();
                if (input % 2 != 0) {
                    outList.add("first list");
                } else {
                    outList.add("second list");
                }
                return outList;
            }
        });

        // 选择奇数流并打印
        splitStream.select("first list").print();
        environment.execute();
    }
}
