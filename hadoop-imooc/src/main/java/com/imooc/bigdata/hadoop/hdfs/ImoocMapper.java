package com.imooc.bigdata.hadoop.hdfs;

/**
 * 自定义Mapper
 */
public interface ImoocMapper {
    /**
     * @param line         读取到每一行数据
     * @param imoocContext 上下文/缓存
     */
    public void map(String line, ImoocContext imoocContext);
}
