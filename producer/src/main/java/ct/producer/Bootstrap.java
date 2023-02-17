package ct.producer;

import ct.common.bean.Producer;
import ct.producer.bean.LocalFileProducer;
import ct.producer.io.LocalFileDataIn;
import ct.producer.io.LocalFileDataOut;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws  Exception {

        if ( args.length < 2 ) {
            System.out.println("系统参数不正确，请按照指定格式传递：java -jar Produce.jar path1 path2 ");
            System.exit(1);
        }

        // 构建生产者对象
        Producer producer = new LocalFileProducer();

        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));

        // 生产数据
        producer.produce();

        // 关闭生产者对象
        producer.close();

    }
}
