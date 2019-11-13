统计页面的浏览量
    select count(1) from xxx;
    一行记录做成一个固定的KEY,value赋值为1

统计各个省份的浏览量
    select province count(1) from xxx group by province;
    地市信息可以通过IP解析得到 <== ip如何转换成城市信息

统计页面的访问量
    把符合规则的PageId获取到，然后进行统计即可

===> 存在的问题：每个MR作业都去读取全量待处理的原始日志，如果数据量很大，疯掉了
===> ETL应运而生

ETL: 全量数据不方便直接进行计算的，最好是进行一步处理后再进行相应的维度统计分析
    解析出你需要的字段：ip==>城市信息
    去除一些你不需要的字段: 不需要的字段就太多了....
    需要的字段 ip、time、url、pageId、country、province、city

大数据处理完以后的数据，目前存放在HDFS之上
其实大数据干的事情基本就是这么多
再进一步：使用技术或者框架把处理完的结果导出到数据库中
    Sqoop：把HDFS上的统计结果导出到Mysql中

