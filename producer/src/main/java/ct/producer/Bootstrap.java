package ct.producer;

import ct.common.bean.Producer;
import ct.producer.bean.LocalFileProducer;
import ct.producer.io.LocalFileDataOut;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws  Exception {

        // 构建生产者对象
        Producer producer = new LocalFileProducer();

        //设置输出文件路径
        producer.setOut(new LocalFileDataOut(args[0]));

        // 生产数据
        producer.produce();

        // 关闭生产者对象
        producer.close();

    }
}
