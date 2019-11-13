package com.imooc.bigdata.hadoop.hdfs;

/**
 * 自定义wc实现类
 */
public class CaseIgnoreWordCountMapper implements ImoocMapper {
    public void map(String line, ImoocContext imoocContext) {
        String[] words = line.split("\t");
        for(String word: words) {
            Object value = imoocContext.get(word);
            if(value == null) { // 没有出现过该单词
                imoocContext.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                imoocContext.write(word, v + 1); // 取出该单词对应的次数并加一
            }

        }
    }
}
