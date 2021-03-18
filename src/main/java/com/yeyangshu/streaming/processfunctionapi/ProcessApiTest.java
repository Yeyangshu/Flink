package com.yeyangshu.streaming.processfunctionapi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 底层API测试：监控每辆汽车，车速超过100km/h，5s后发出超速警告
 * 越低层次的API功能越强大，用户能获取的信息越多
 *
 * 定时器应用场景：数据延迟
 * 对账系统，银行对账
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/18 22:00
 */
public class ProcessApiTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = environment.socketTextStream("localhost", 8888);

        stream
                .map(new MapFunction<String, CarInfo>() {
                    @Override
                    public CarInfo map(String input) throws Exception {
                        String[] splits = input.split(" ");
                        String carId = splits[0];
                        Long speed = Long.valueOf(splits[1]);
                        CarInfo carInfo = new CarInfo(carId, speed);
                        return carInfo;
                    }
                })
                .keyBy("cardId")
                .process(new KeyedProcessFunction<Tuple, CarInfo, String>() {
                    @Override
                    public void processElement(CarInfo carInfo, Context context, Collector<String> collector) throws Exception {
                        long currentTime = context.timerService().currentProcessingTime();
                        if (carInfo.speed > 100) {
                            long timerTime = currentTime + 5 * 1000;
                            context.timerService().registerProcessingTimeTimer(timerTime);
                        }
                    }

                    // 定时器应用场景如注释描述
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        String warnMessage = "warn! time: " + timestamp + ", car id: " + ctx.getCurrentKey();
                        out.collect(warnMessage);
                    }
                });
    }

    public static class CarInfo {
        private String carId;
        private Long speed;

        public CarInfo(String carId, Long speed) {
            this.carId = carId;
            this.speed = speed;
        }
    }
}
