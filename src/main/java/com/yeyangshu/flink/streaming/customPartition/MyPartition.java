package com.yeyangshu.flink.streaming.customPartition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区规则，根据数值奇偶性分区
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/9 23:40
 */
public class MyPartition implements Partitioner<Long>  {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数：" + numPartitions);
        if (key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
