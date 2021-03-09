package com.yeyangshu.flink.streaming.customsource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义实现并行度为1的Source
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/9 23:28
 */
public class MyNoParallelSource implements SourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;

    /**
     * 启动一个Source
     *
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
            sourceContext.collect(count);
            count++;
            // 每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    /**
     * 执行cancel操作时会调用的方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
