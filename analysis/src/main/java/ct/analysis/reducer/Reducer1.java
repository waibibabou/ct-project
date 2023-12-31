package ct.analysis.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 统计效用值
 */
public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
    private IntWritable outV=new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //整合效用值
        int sum=0;
        for(IntWritable intWritable:values){
            sum+=intWritable.get();
        }
        outV.set(sum);

        context.write(key,outV);
    }
}
