package hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.swing.tree.ExpandVetoException;
import java.io.*;
import java.net.URI;
import java.nio.*;


public class HdfsTest {
    public Configuration conf = null;
    public FileSystem fs = null;


    @Before
    public void conn() throws Exception {
        conf = new Configuration(true);
        fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "root");
    }


    @Test
    public void mkdir() throws Exception {
        Path dir = new Path("/user/root");
        if (fs.exists((dir))) {
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }

    @Test
    public void upload() throws Exception {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
        Path outFile = new Path("/chen/out.txt");
        FSDataOutputStream output = fs.create(outFile);

        IOUtils.copyBytes(input, output, conf, true);
    }

    @Test
    public void blocks() throws Exception {
        Path file = new Path("/chen/out.txt");
        FileStatus fss = fs.getFileStatus(file);
        BlockLocation[] blks = fs.getFileBlockLocations(fss, 0, fss.getLen());
        for (BlockLocation b : blks) {
            System.out.println(b);
        }
        FSDataInputStream in = fs.open(file);
        //in.seek(1048576);
        byte[] bytes = new byte[20];
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        while (reader.ready()) {
            System.out.println(reader.readLine());
        }


    }

    @After
    public void close() throws Exception {
        fs.close();
    }




}
