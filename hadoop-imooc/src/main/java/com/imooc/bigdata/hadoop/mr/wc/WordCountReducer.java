package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * reducer和mapper中其实使用到了什么设计模式：模板设计模式
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * (hello, 1)
     * (world, 1)
     * (hello, 1) (world, 1)
     * (hello, 1) (world, 1)
     * (welcome, 1)
     * map的输出到reduce端，是按照相同的key分发到一个reduce
     *
     * reduce1：(hello, 1)(hello, 1)(hello, 1) ==> (hello, <1,1,1>)
     * reduce2：(world, 1)(world, 1) ==> (world, <1, 1>)
     * reduce3：(welcome, 1) ==> (welcome, <1>)
     *
     * @param key 对应的单词，hello等
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntWritable next = iterator.next();
            count += next.get();
        }
        context.write(key, new IntWritable(count));
    }
}
