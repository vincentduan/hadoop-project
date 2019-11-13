package vincent;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RenyuankuMapper extends Mapper<LongWritable, Text, Text, Renyuanku> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String uid = split[0];
        String data = split[3];
        JSONObject jsonObject = JSONObject.parseObject(data);
        String imei = "";
        String imsi = "";
        String mac = "";
        String msisdn = "";
        if(jsonObject.containsKey("imei")) {
            imei = jsonObject.getString("imei");
        }
        if(jsonObject.containsKey("imsi")) {
            imsi = jsonObject.getString("imsi");
        }
        if(jsonObject.containsKey("mac")) {
            mac = jsonObject.getString("mac");
        }
        if(jsonObject.containsKey("msisdn")) {
            msisdn = jsonObject.getString("msisdn");
        }

        context.write(new Text(uid), new Renyuanku(uid, imei, imsi, mac, msisdn));

    }
}
