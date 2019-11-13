package com.imooc.bigdata.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 使用HDFS API完成wordcount词频统计
 * 需求： 统计HDFS上的文件的WC, 然后将统计结果输出到HDFS
 * <p>
 * 功能拆解：
 * 1) 读取HDFS上的文件 ===> HDFS API
 * 2) 业务处理(词频处理): 对文件中的每一行数据都要进行业务处理(按照分隔符分割) ===> Mapper
 * 3) 将处理结果缓存 ===> Context
 * 4) 将结果输出到HDFS ===> HDFS API
 */
public class HDFSWCApp01 {
    public static void main(String[] args) throws Exception {
        // 1. 读取HDFS上的文件 ===> HDFS API
        Path input = new Path("/hdfsapi/test/helloworld.txt");

        // 获取要操作的HDFS文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://swarm-worker1:9000/"), new Configuration(), "iie4bu");
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(input, false);
        ImoocMapper mapper = new WordCountMapper();
        // 3. 将结果缓存起来
        ImoocContext imoocContext = new ImoocContext();
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = reader.readLine()) != null) {
                // 2. 业务处理(词频处理)词频处理
                mapper.map(line, imoocContext);
                // TODO... 在业务逻辑处理完之后将结果写到Cache中去
            }
            reader.close();
            reader.close();
        }


        // 3. 获取缓存
        Map<Object, Object> contextMap = imoocContext.getCacheMap();

        // 4. 将结果输出到HDFS ===> HDFS API
        Path outPath = new Path("/hdfsapi/output/");
        FSDataOutputStream out = fs.create(new Path(outPath, new Path("wc.out")));

        // 将第三步缓存中的内容输出到out中去
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for(Map.Entry<Object, Object> entry: entries) {
            out.write((entry.getKey().toString() + ": " + entry.getValue().toString() + "\n").getBytes());
        }
        out.close();
        fs.close();
        System.out.println("执行完毕");
    }
}
