package ct.common.bean;

import ct.common.api.Column;
import ct.common.api.Rowkey;
import ct.common.api.TableRef;
import ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * 基础数据访问对象
 */
public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() throws Exception {
        getConnection();
        getAdmin();
    }

    protected  void end() throws Exception {
        Admin admin = getAdmin();
        if ( admin != null ) {
            admin.close();
            adminHolder.remove();
        }

        Connection conn = getConnection();
        if ( conn != null ) {
            conn.close();
            connHolder.remove();
        }
    }

    /**
     * 创建表，如果表已经存在，那么删除后在创建新的
     * @param name
     * @param regionCount
     */
    protected void createTableXX( String name, Integer regionCount) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        if ( admin.tableExists(tableName) ) {
            // 表存在，删除表
            deleteTable(name);
        }
        // 创建表
        createTable(name, regionCount, null);
    }

    private void createTable( String name, Integer regionCount, String... families ) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);

        HTableDescriptor tableDescriptor =
            new HTableDescriptor(tableName);

        if ( families == null || families.length == 0 ) {
            families = new String[1];
            families[0] = Names.CF_INFO.getValue();
        }

        for (String family : families) {
            HColumnDescriptor columnDescriptor =
                new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }

        // 增加预分区
        if ( regionCount == null || regionCount <= 1 ) {
            admin.createTable(tableDescriptor);
        } else {
            // 分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor, splitKeys);
        }
    }

    /**
     * 计算分区号(0, 1, 2, 3, 4, 5)
     * @param firstcode
     * @return
     */
    protected int genRegionNum( String firstcode) {

        int CodeHash = firstcode.hashCode();
        // 取模
        int regionNum = CodeHash % 6;

        return regionNum;

    }

    /**
     * 生成分区键
     * @return
     */
    private byte[][] genSplitKeys(int regionCount) {

        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];
        // 0|,1|,2|,3|,4|
        // (-∞, 0|), [0|,1|), [1| +∞)
        List<byte[]> bsList = new ArrayList<byte[]>();
        for ( int i = 0; i < splitKeyCount; i++ ) {
            String splitkey = i + "|";
            bsList.add(Bytes.toBytes(splitkey));
        }
        bsList.toArray(bs);

        return bs;
    }


    /**
     * 增加一条数据
     * @param name
     * @param put
     */
    protected void putData( String name, Put put ) throws Exception {

        // 获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));

        // 增加数据
        table.put(put);

        // 关闭表
        table.close();
    }

    /**
     * 删除表格
     * @param name
     * @throws Exception
     */
    protected void deleteTable(String name) throws Exception {
        TableName tableName = TableName.valueOf(name);
        Admin admin = getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 创建命名空间，如果命名空间已经存在，不需要创建，否则，创建新的
     * @param namespace
     */
    protected void createNamepsaceNX( String namespace ) throws Exception {
        Admin admin = getAdmin();

        try {
            admin.getNamespaceDescriptor(namespace);
        } catch ( NamespaceNotFoundException e ) {
            NamespaceDescriptor namespaceDescriptor =
                NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }

    }

    /**
     * 获取连接对象
     */
    protected synchronized Admin getAdmin() throws Exception {
        Admin admin = adminHolder.get();
        if ( admin == null ) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 获取连接对象
     */
    protected synchronized Connection getConnection() throws Exception {
        Connection conn = connHolder.get();
        if ( conn == null ) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }

}
