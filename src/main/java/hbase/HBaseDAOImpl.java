package hbase;

import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HBaseDAOImpl {
    static Configuration conf = null;
    Connection conn = null;

    public HBaseDAOImpl() {
        conf = new Configuration();
        String zk_list = "node3,node4,node5";
        conf.set("hbase.zookeeper.quorum", zk_list);
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void save(Put put, String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void save(List<Put> Put, String tableName) {
        //批量
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            table.put(Put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void insert(String tableName, String rowKey, String family,
                       String qualifier, String value) {
        //
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes(StandardCharsets.UTF_8));
            put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void insert(String tableName, String rowKey, String family, String[] qualifier, String[] value){
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            if (qualifier.length != value.length) throw new IOException();
            for (int i = 0; i < qualifier.length; i ++) {
                put.addColumn(family.getBytes(), qualifier[i].getBytes(), value[i].getBytes());
            }
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Result getOneRow(String tableName, String rowKey) {
        Table table = null;
        Result result = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            result = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public Result getOneRowAndMultiColumn(String tableName, String rowKey, String[] cols) {
        Table table = null;
        Result result = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            for (int i = 0; i < cols.length; i ++) {
                get.addColumn("cf".getBytes(), cols[i].getBytes(StandardCharsets.UTF_8));

            }
            result = table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public List<Result> getRows(String tableName, String rowKeyLike) {
        Table table = null;
        List<Result> list = null;
        try {
            FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            table = conn.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                    "order".getBytes(),
                    "order_type".getBytes(),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("1")
            );//等于1的列
            fl.addFilter(filter);
            fl.addFilter(filter1);

            Scan scan = new Scan();
            scan.setFilter(fl);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner){
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public List<Result> getRows(String tableName, String rowKeyLike, String[] cols) {
        Table table = null;
        List<Result> list = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());

            Scan scan = new Scan();
            for (int i = 0; i < cols.length; i ++) {
                scan.addColumn("cf".getBytes(), cols[i].getBytes());
            }
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e){
                e.printStackTrace();
            }
        }
        return list;
    }

    public List<Result> getRows(String tableName, String startRow, String stopRow) {
        Table table = null;
        List<Result> list = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(stopRow.getBytes());
            ResultScanner resultScanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner) list.add(rs);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public void deleteRecords(String tableName,String rowKeyLike) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes(StandardCharsets.UTF_8));
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            ArrayList<Delete> deletes = new ArrayList<>();
            for (Result result: scanner) {
                Delete delete = new Delete(result.getRow());
                deletes.add(delete);
            }
            table.delete(deletes);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void deleteCell(String tableName, String rowKey, String cf, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName.getBytes()));
        Delete delete = new Delete(rowKey.getBytes(StandardCharsets.UTF_8));
        delete.addColumn(cf.getBytes(),column.getBytes());
        table.delete(delete);
        try {
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void createTable(String tableName, String[] columnFamilies) {
        try {
            Admin admin = conn.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName.getBytes()))){
                System.out.println("this table is existed");
            } else {
                TableDescriptorBuilder tableDescriptorBuilder =
                        TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                for (String family : columnFamilies) {
                    tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.
                            newBuilder(family.getBytes()).build());//底层map
                }
                admin.createTable(tableDescriptorBuilder.build());
                System.out.println("success!");
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteTable(String tableName) {
        TableName table = TableName.valueOf(tableName.getBytes());
        try {
            Admin admin = conn.getAdmin();
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
                System.out.println("success");
            } else {
                System.out.println("table doesn't exist");
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void scan(String tableName) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();
            ResultScanner rs = table.getScanner(s);

            for (Result r : rs) {
                for (Cell cell : r.rawCells()) {
                    System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                    System.out.println("Timestamp:" + cell.getTimestamp() + " ");
                    System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                    System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
                    System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void scanByColumn(String tableName, String qualifier) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.addColumn("cf".getBytes(), qualifier.getBytes());
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs){
                for (Cell cell : r.rawCells()) {
                    System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                    System.out.println("Timestamp:" + cell.getTimestamp() + " ");
                    System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                    System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
                    System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }
}
