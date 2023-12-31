package ct.consumer;

import ct.common.bean.Consumer;
import ct.consumer.bean.CalllogConsumer;

/**
 * 启动消费者

    使用Kafka消费者获取Flume采集的数据

    并将数据存储到Hbase中去
 */
public class Bootstrap {
    public static void main(String[] args) throws  Exception {

        // 创建消费者
        Consumer consumer = new CalllogConsumer();

        // 消费数据
        consumer.consume();

        // 关闭资源
        consumer.close();

    }
}
