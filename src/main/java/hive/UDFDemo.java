package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UDFDemo extends UDF{

    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }
        String str = s.toString().substring(0, 1) + "chen";
        return new Text(str);
    }
}
