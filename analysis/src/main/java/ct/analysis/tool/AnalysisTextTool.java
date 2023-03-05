package ct.analysis.tool;

import ct.analysis.io.MySQLTextOutputFormat;
import ct.analysis.mapper.Mapper1;
import ct.analysis.mapper.Mapper2;
import ct.analysis.reducer.Reducer1;
import ct.analysis.reducer.Reducer2;
import ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.Path;

import java.util.Random;

/**
 * 分析数据的工具类Driver
 */
public class AnalysisTextTool implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job1 = Job.getInstance(conf,"utility count 1");
        job1.setJarByClass(AnalysisTextTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_INFO.getValue()));

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue(),
                scan,
                Mapper1.class,
                Text.class,
                IntWritable.class,
                job1
        );

        // reducer
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath((JobConf) job1.getConfiguration(),new Path("hdfs:/output"));

        boolean flg = job1.waitForCompletion(true);

        if ( flg ) {
            Configuration confs=new Configuration();
            Job job2=Job.getInstance(confs,"utility count2");
            job2.setJarByClass(AnalysisTextTool.class);
            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);

            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths((JobConf) job2.getConfiguration(),new Path("hdfs:/output/part-r-00000"));
//            FileOutputFormat.setOutputPath((JobConf) job2.getConfiguration(),new Path("hdfs:/output2"));//输出到文件
            job2.setOutputFormatClass(MySQLTextOutputFormat.class);//输出到mysql

            boolean b=job2.waitForCompletion(true);
            //完成后删除第一步的未经排序的结果
            if(b){
                FileSystem fs=FileSystem.get(conf);
                fs.deleteOnExit(new Path("hdfs:/output"));
            }

        }
        return JobStatus.State.SUCCEEDED.getValue();
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
