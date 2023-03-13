package ct.analysis.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer2 extends Reducer<IntWritable, Text,Text,Text> {
    private Text outK=new Text();
    private Text outV=new Text();


    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        for(Text text:values){
            String line=text.toString();
            StringBuilder sb=new StringBuilder();
            String[] l=line.split(" ");
            //将格式统一为code+2位的车厢号 如果车厢号为1位则补零
            for(int i=0;i<l.length;i++){
                sb.append(l[i].substring(0,4));
                sb.append(l[i].length()==6?("0"+l[i].charAt(l[i].length()-1)):l[i].substring(5,7));
            }

            outK.set(sb.toString());
            //mapper2传过来的是负数，这里再次取反得到原始值
            outV.set(String.valueOf(-key.get()));
            //写出到mysql中
            context.write(outK,outV);
        }

    }
}
