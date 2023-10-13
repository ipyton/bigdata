package mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNGroupingComparator extends WritableComparator {
    //决定哪些会被放在一个reduce里面进行迭代，注意是先排序再进行分组。分组是线性判断,所以会出现断的情况

    public int compare(WritableComparable a, WritableComparable b) {
        TopNKey k1 = (TopNKey)a;
        TopNKey k2 = (TopNKey)b;
        //  按着 年，月分组
        int c1 = Integer.compare(k1.getYear(), k2.getYear());
        if(c1 == 0 ){
            return  Integer.compare(k1.getMonth(), k2.getMonth());
        }

        return c1;
    }
}
