package com.imooc.bigdata.hadoop.mr.project.utils;

import org.junit.Test;

public class IpTest {

    @Test
    public void testIp() {
        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("159.226.95.74");
        System.out.println(regionInfo.getCity());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCountry());

    }

}
