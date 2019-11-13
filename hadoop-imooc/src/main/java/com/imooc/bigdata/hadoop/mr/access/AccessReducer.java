package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AccessReducer  extends Reducer<Text, Access, Text, Access> {

    /**
     *
     * @param key 手机号
     * @param values <Access, Access>
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long ups = 0;
        long downs = 0;
        for(Access access: values) {
            ups = access.getUp();
            downs = access.getDown();
        }
        context.write(key, new Access(key.toString(), ups, downs));
    }
}
