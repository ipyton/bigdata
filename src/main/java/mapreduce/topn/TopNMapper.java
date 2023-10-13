package mapreduce.topn;

import com.sun.crypto.provider.HmacSHA1KeyGenerator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class TopNMapper extends Mapper<LongWritable, Text, TopNKey, IntWritable> {
    TopNKey key = new TopNKey();
    IntWritable mval = new IntWritable();

    public HashMap<String, String> dict = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TopNKey, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] strs = StringUtils.split(value.toString(), '\t');
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");


        try {
            Date date = sdf.parse(strs[0]);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            this.key.setYear(cal.get(Calendar.YEAR));
            this.key.setMonth(cal.get(Calendar.MONTH));
            this.key.setDay(cal.get(Calendar.DAY_OF_MONTH));
            int temperature = Integer.parseInt(strs[2]);
            this.key.setTemperature(temperature);
            this.key.setLocation(dict.get(strs[1]));
            mval.set(temperature);
            context.write(this.key, mval);
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }


    @Override
    protected void setup(Mapper<LongWritable, Text, TopNKey, IntWritable>.Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();
        Path path = new Path(files[0].getPath());
        BufferedReader reader = new BufferedReader(new FileReader(new File(path.getName())));

        String line = reader.readLine();

        while (line != null) {
            String[] split = line.split("\t");
            dict.put(split[0], split[1]);
            line = reader.readLine();
        }
    }
}
