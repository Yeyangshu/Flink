package com.yeyangshu.streaming.sink;

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
    public static void main(String[] args) {
        
    }
}
