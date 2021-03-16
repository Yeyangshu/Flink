package com.yeyangshu.streaming.transformation;

/**
 * 现有一个配置文件存储车牌号与车主的真实姓名
 * 通过数据流中的车牌号实时匹配对应的车主姓名
 * （注意：配置文件实时改变）
 * 配置文件可能实时改变，读取配置文件的方法：readFile()，readTextFile（仅读取一次）
 * stream1.connect(stream2)
 *
 * @author zhumingxing
 * @date 2021/3/15 22:02
 **/
public class Demo3_CoFlatMap {
}
