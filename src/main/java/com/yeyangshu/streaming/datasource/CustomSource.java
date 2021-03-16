package com.yeyangshu.streaming.datasource;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义数据源
 * SourceFunction：只支持单并行度
 * ParallelSourceFunction：支持多并行度数据源
 *
 * @author zhumingxing
 * @date 2021/3/11 23:23
 **/
public class CustomSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // SourceFunction中的泛型代表发送数据的类型
        environment.addSource(new SourceFunction<String>() {
            boolean flag = true;
            // 产生数据
            // 实际run可以读取任何地方的数据，将数据发送出去，例如Redis
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    Random random = new Random();
                    while (flag) {
                        ctx.collect("hello " + random.nextInt(1000));
                        Thread.sleep(500);
                    }
                }
            }

            // 停止
            @Override
            public void cancel() {
                flag = false;
            }
        }).print();

        environment.execute();
    }
}
