package ct.analysis.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, Text,IntWritable, Text> {
    private IntWritable outK=new IntWritable();
    private Text outV=new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line=value.toString();
        //切割
        String[] words = line.split("\t");
        int utility=Integer.parseInt(words[1]);
        //效用值取反是因为要从大到小排序，但是默认是从小到大所以取负数
        outK.set(-utility);
        outV.set(words[0]);
        context.write(outK,outV);


    }
}

