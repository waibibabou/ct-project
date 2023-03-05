package ct.producer.bean;

import ct.common.bean.DataOut;
import ct.common.bean.Producer;

import java.io.IOException;
import java.util.Random;

/**
 * 本地数据文件生产者
 */
public class LocalFileProducer implements Producer {

    private DataOut out;
    private volatile boolean flg = true;

    public void setOut(DataOut out) {
        this.out = out;
    }

    /**
     * 生产数据
     */
    public void produce() {

        try {
            while ( flg ) {
                Random r=new Random();
                int count=r.nextInt(10)+1;
                StringBuilder sb1=new StringBuilder();
                StringBuilder sb2=new StringBuilder();
                int total=0;
                for(int i=0;i<count;i++){
                    sb1.append(r.nextInt(8000) + 1000+"|"+r.nextInt(10));
                    if(i!=count-1)sb1.append(" ");

                    int t=r.nextInt(20)+1;
                    total+=t;
                    sb2.append(t);
                    if(i!=count-1)sb2.append(" ");
                }


                String s= sb1 +":"+total+":"+ sb2;
                System.out.println(s);
                // 将通话记录刷写到数据文件中
                out.write(s);

                Thread.sleep(500);
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭生产者
     * @throws IOException
     */
    public void close() throws IOException {
        if ( out != null ) {
            out.close();
        }
    }
}
