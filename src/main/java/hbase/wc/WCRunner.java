package hbase.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WCRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(true);
        conf.set("hbase.zookeeper.quorum","node4,node2,node3");
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WCRunner.class);

        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        FileInputFormat.addInputPath(job, new Path("/wc"));//source
        job.waitForCompletion(true);


    }
}
