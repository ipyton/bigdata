package hbase.wc;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i: values) {
            sum+=i.get();
        }
        Put put = new Put(Bytes.toBytes(key.toString()));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("ct"),Bytes.toBytes(String.valueOf(sum)));
        context.write(null,put);
    }
}
