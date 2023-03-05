package ct.consumer.dao;

import ct.common.bean.BaseDao;
import ct.common.constant.Names;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Random;

/**
 * Hbase数据访问对象
 */
public class HBaseDao extends BaseDao {
    /**
     * 初始化
     * 创建命名空间与表
     */
    public void init() throws Exception {
        start();

        createNamepsaceNX(Names.NAMESPACE.getValue());
        //设置表格分区数为6
        createTableXX(Names.TABLE.getValue(),6);

        end();
    }

    /**
     * 插入数据
     * @param value
     */
    public void insertData(String value) throws Exception {

        // 将故障日志保存到Hbase表中

        String data=value;
        Random r=new Random();
        String firstcode=data.substring(0,4);
        // rowkey = regionNum + data + 4位的随机数 加入随机数是为了保证rowkey的唯一性
        String rowkey = genRegionNum(firstcode) + "_" +data+"_"+(1000+r.nextInt(5000));
        Put put = new Put(Bytes.toBytes(rowkey));

        byte[] family = Bytes.toBytes(Names.CF_INFO.getValue());
        //列族为info 列名为data
        put.addColumn(family, Bytes.toBytes("data"), Bytes.toBytes(data));

        //保存数据
        putData(Names.TABLE.getValue(), put);

    }
}
