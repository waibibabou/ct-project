package ct.analysis.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * 分析数据Mapper
 */
public class Mapper1 extends TableMapper<Text, IntWritable> {
    private Text outK=new Text();
    private IntWritable outV=new IntWritable();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String rowkey = Bytes.toString(key.get());
        //去掉rowkey中前面的 分区号_ 以及后面的 _4位随机数
        String line=rowkey.substring(2,rowkey.length()-5);
        int index1=line.indexOf(":");
        int index2=line.lastIndexOf(":");
        String substr1=line.substring(0,index1);
        String substr2=line.substring(index2+1);
        String[] l1=substr1.split(" ");
        String[] l2=substr2.split(" ");
        HashMap<String,Integer> map=new HashMap<>();
        for(int i=0;i<l1.length;i++){
            map.put(l1[i],Integer.parseInt(l2[i]));
        }
        //排序，使得（s1,s2）与（s2,s1）不会同时出现在结果中
        Arrays.sort(l1);

        List<List<String>> ans=new ArrayList<>();
        dfs(0,new ArrayList<>(),l1,ans);
        for(int i=0;i<ans.size();i++){
            List<String>t=ans.get(i);
            int temp=0;
            StringBuffer str=new StringBuffer();
            for(int j=0;j<t.size();j++){
                temp+=map.get(t.get(j));
                str.append(t.get(j));
                if(j!=t.size()-1){
                    str.append(" ");
                }
            }
            outK.set(String.valueOf(str));
            outV.set(temp);
            if(temp!=0) context.write(outK,outV);
        }

    }


    public void dfs(int cur, List<String> current, String[] l1, List<List<String>> ans){
        if(cur==l1.length){
            ans.add(new ArrayList<>(current));
            return;
        }
        current.add(l1[cur]);
        dfs(cur+1,current,l1,ans);
        current.remove(current.size()-1);
        dfs(cur+1,current,l1,ans);

    }

}
