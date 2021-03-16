package com.yeyangshu.streaming;

/**
 * @author zhumingxing
 * @date 2021/3/11 13:56
 **/
public class WorldCount {
    public String word;
    public Long count;

    public WorldCount() {
    }

    public WorldCount(String word, Long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "WorldCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
