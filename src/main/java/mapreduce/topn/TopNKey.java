package mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopNKey implements WritableComparable<TopNKey> {

    private int year;
    private int month;
    private int day;
    private int temperature;

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    private String location;

    public TopNKey() {
    }


    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }



    @Override
    public int compareTo(TopNKey that) {
        int c1 = Integer.compare(this.year, that.year);
        if(c1 == 0) {
            int c2 = Integer.compare(this.month, that.month);
            if(c2 == 0) {
                return Integer.compare(this.day, that.day);
            }
            return c2;

        }
        return c1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(temperature);
        out.writeUTF(location);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month=in.readInt();
        this.day=in.readInt();
        this.temperature=in.readInt();
        this.location = in.readUTF();
    }
}
