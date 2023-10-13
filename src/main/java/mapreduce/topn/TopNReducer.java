package mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TopNReducer extends Reducer<TopNKey, IntWritable, Text, IntWritable> {

    Text key = new Text();
    IntWritable val = new IntWritable();


    @Override
    protected void reduce(TopNKey key, Iterable<IntWritable> values, Reducer<TopNKey, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iter = values.iterator();

        int flag = 0;

        while (iter.hasNext() && flag <= 1) {
            IntWritable val = iter.next();
            this.key.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "@" + key.getLocation());
            val.set(key.getTemperature());
            context.write(this.key, val);
            flag ++;
        }
    }
}
