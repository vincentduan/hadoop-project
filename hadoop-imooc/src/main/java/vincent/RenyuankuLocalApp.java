package vincent;

import com.imooc.bigdata.hadoop.hdfs.Constants;
import com.imooc.bigdata.hadoop.hdfs.ParamsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RenyuankuLocalApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(RenyuankuLocalApp.class);

        job.setMapperClass(RenyuankuMapper.class);
        job.setReducerClass(RenyuankuReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Renyuanku.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("file:/home/iie4bu/SD_DATA/internet_user_daily_hk-20190731-20190810"));
        FileOutputFormat.setOutputPath(job, new Path("file:/home/iie4bu/SD_DATA/output"));

        job.waitForCompletion(true);
    }
}
