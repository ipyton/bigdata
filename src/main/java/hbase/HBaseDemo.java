package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HBaseDemo {
    Configuration conf = null;
    Connection conn = null;

    Admin admin = null;
    Table table =null;

    TableName tableName = TableName.valueOf("phone");

    @Before
    public void init() throws IOException {
        //创建配置文件对象
        conf = HBaseConfiguration.create();
        //config zookeeper
        conf.set("hbase.zookeeper.quorum","node3,node4,node5");
        //获取连接
        conn = ConnectionFactory.createConnection(conf);
        //获取对象
        admin = conn.getAdmin();
        table = conn.getTable(tableName);
    }

    @Test
    public void createTable() throws IOException {
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
        tableDescriptorBuilder.setColumnFamily(builder.build());
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.createTable(tableDescriptorBuilder.build());
    }

    @Test
    public void insert(String tableName, String name, int age, String gender) throws IOException {
        Put put = new Put(tableName.getBytes());
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(age));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(gender), Bytes.toBytes(gender));
        table.put(put);
    }

    @Test
    public void get(String table) throws IOException {
        Get get = new Get(table.getBytes());
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"));
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"));

        Result result = this.table.get(get);

        Cell cell1 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        Cell cell2 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("age"));
        Cell cell3 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("sex"));
        String name = cell1.toString();
        String age = cell2.toString();
        String sex = cell3.toString();
        System.out.println(name + age + sex);
    }

    @Test
    public void scan() throws IOException {
        Scan scan = new Scan();
        ResultScanner rss = table.getScanner(scan);
        for (Result rs: rss) {
            Cell cell1 = rs.getColumnLatestCell(Bytes.toBytes("cf"),Bytes.toBytes("name"));
            Cell cell2 = rs.getColumnLatestCell(Bytes.toBytes("cf"),Bytes.toBytes("age"));
            Cell cell3 = rs.getColumnLatestCell(Bytes.toBytes("cf"),Bytes.toBytes("sex"));
            String name = Bytes.toString(CellUtil.cloneValue(cell1));
            String age = Bytes.toString(CellUtil.cloneValue(cell2));
            String sex = Bytes.toString(CellUtil.cloneValue(cell3));
            System.out.println(name);
            System.out.println(age);
            System.out.println(sex);
        }
    }

    @After
    public void destroy(){
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }





}
