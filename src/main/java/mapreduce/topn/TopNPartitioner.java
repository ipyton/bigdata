package mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopNPartitioner extends Partitioner<TopNKey, IntWritable> {
    //对数据进行分区的规则
    @Override
    public int getPartition(TopNKey topNKey, IntWritable intWritable, int numPartitions) {
        return topNKey.getYear() % numPartitions;
    }
}
