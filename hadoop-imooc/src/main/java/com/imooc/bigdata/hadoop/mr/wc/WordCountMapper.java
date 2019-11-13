package com.imooc.bigdata.hadoop.mr.wc;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: Map任务读数据的key类型，offset，每行数据起始位置的偏移量，一般是Long类型
 * VALUEIN: Map任务读数据的value类型。其实就是一行行的字符串，一般是String类型
 * <p>
 * hello world welcome
 * hello welcome
 * <p>
 * KEYOUT: Map方法自定义实现输出的key的类型, String
 * VALUEOUT: Map方法自定义实现输出的value的类型, Integer
 * <p>
 * 词频统计：相同单词的次数 (word,1)
 * 不建议这么写：<Long, String ,String, Integer>，因为这些类型是Java中的数据类型，在Hadoop中有自定义的类型，因为分布式计算很多都是要经过网络传输的
 * 因此一定要涉及到序列化与反序列化操作。
 * 因此Hadoop中的自定义类型是支持序列化和反序列化的，并且比Java中实现的性能更好。
 * LongWritable，Text
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     *
     * @param key 偏移量
     * @param value 一行数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 第一步：将value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split(" ");
        for(String word: words) {
            // (hello, 1) (world, 1)
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }

    }
}
