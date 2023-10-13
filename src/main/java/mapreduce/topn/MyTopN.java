package mapreduce.topn;

import mapreduce.wordcount.WCMapper;
import mapreduce.wordcount.WCReducer;
import mapreduce.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyTopN {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(true);
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);  //工具类帮我们把-D 等等的属性直接set到conf，会留下commandOptions
        String[] othargs = parser.getRemainingArgs();

        //让框架知道是windows异构平台运行
        conf.set("mapreduce.app-submission.cross-platform","true");

//        conf.set("mapreduce.framework.name","local");
//        System.out.println(conf.get("mapreduce.framework.name"));

        Job job = Job.getInstance(conf);
        job.addCacheFile(new Path("/data/topn/dict/dict.txt").toUri());
        job.setJar("E:\\IJProjects\\bigdata\\target\\hadoop-hdfs-1.0-0.1.jar");
        //必须必须写的
        job.setJarByClass(MyTopN.class);

        job.setJobName("topn");

        Path infile = new Path(othargs[0]);
        TextInputFormat.addInputPath(job, infile);

        Path outfile = new Path(othargs[1]);
        if (outfile.getFileSystem(conf).exists(outfile)) outfile.getFileSystem(conf).delete(outfile, true);
        TextOutputFormat.setOutputPath(job, outfile);




        job.setMapperClass(TopNMapper.class);
        job.setPartitionerClass(TopNPartitioner.class);
        job.setSortComparatorClass(TopNSortComparator.class);
        job.setMapOutputKeyClass(TopNKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setGroupingComparatorClass(TopNGroupingComparator.class);
        job.setReducerClass(TopNReducer.class);



//        job.setNumReduceTasks(2);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);


    }
}
