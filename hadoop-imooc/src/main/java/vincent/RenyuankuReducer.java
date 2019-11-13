package vincent;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class RenyuankuReducer extends Reducer<Text, Renyuanku, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Renyuanku> values, Context context) throws IOException, InterruptedException {
        Set<String> imei = new HashSet<>();
        Set<String> imsi = new HashSet<>();
        Set<String> mac = new HashSet<>();
        Set<String> msisdn = new HashSet<>();

        for (Renyuanku renyuanku : values) {
            imei = inputToSet(renyuanku.getImei());
            imsi = inputToSet(renyuanku.getImsi());
            mac = inputToSet(renyuanku.getMac());
            msisdn = inputToSet(renyuanku.getMsisdn());
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("uid", key.toString());
        if (imei.size() > 0) {
            jsonObject.put("imei", imei);
        }
        if (imsi.size() > 0) {
            jsonObject.put("imsi", imsi);
        }
        if (mac.size() > 0) {
            jsonObject.put("mac", mac);
        }
        if (msisdn.size() > 0) {
            jsonObject.put("msisdn", msisdn);
        }
        context.write(key, new Text(jsonObject.toJSONString()));
    }

    public Set<String> inputToSet(String inputMaZhi) {
        Set<String> mazhi = new HashSet<>();
        if (!"".equals(inputMaZhi)) {
            if (inputMaZhi.contains("<>")) {
                String[] split = inputMaZhi.split("<>");
                mazhi.addAll(Arrays.asList(split));
            } else {
                mazhi.add(inputMaZhi);
            }
        }
        return mazhi;
    }
}
