package ct.analysis.io;

import ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 将MR结果插入到mysql的12个表中
 */
public class MySQLTextOutputFormat extends OutputFormat<Text, Text> {

    protected static class MySQLRecordWriter extends RecordWriter<Text, Text> {

        private Connection connection = null;
        Map<String, String> codeMap = new HashMap<>();
        Map<String, Integer> systemToBiaoMap=new HashMap<>();

        public MySQLRecordWriter() {
            //添加故障与表的对应map
            systemToBiaoMap.put("车门及车端连接",1);
            systemToBiaoMap.put("车内环境系统",2);
            systemToBiaoMap.put("车内设施",3);
            systemToBiaoMap.put("辅助及充电机系统",4);
            systemToBiaoMap.put("高压及牵引系统",5);
            systemToBiaoMap.put("空调系统",6);
            systemToBiaoMap.put("列控车载设备",7);
            systemToBiaoMap.put("旅客信息系统",8);
            systemToBiaoMap.put("网络系统",9);
            systemToBiaoMap.put("烟火系统",10);
            systemToBiaoMap.put("制动系统",11);
            systemToBiaoMap.put("转向架系统",12);

            // 获取字典 codeMap中存储故障code以及对应的故障大类
            connection = JDBCUtil.getConnection();
            PreparedStatement pstat = null;
            ResultSet rs = null;

            try {

                String queryUserSql = "select `code`,`system` from dictionary";
                pstat = connection.prepareStatement(queryUserSql);
                rs = pstat.executeQuery();
                while ( rs.next() ) {
                    String code = rs.getString(1);
                    String system = rs.getString(2);
                    codeMap.put(code, system);
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if ( rs != null ) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if ( pstat != null ) {
                    try {
                        pstat.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * 输出数据
         * @param key
         * @param value
         */
        public void write(Text key, Text value) throws IOException,InterruptedException{
            PreparedStatement pstat = null;
            String line=key.toString();
            String utility=value.toString();
            //每个故障的存储格式为 code+车厢 都是6位
            int count=line.length()/6;
            //遍历这行数据中所有故障，如有5条故障则分别存储5次
            for(int i=0;i<count;i++){
                String guZhangAndCheXiang=line.substring(6*i,6*(i+1));
                //待插入的数据，效用前加0表示接下来是效用值了
                String temp=line.substring(0,6*i)+ line.substring(6*(i+1))+"0"+utility;
                String guZhang=line.substring(6*i,6*i+4);
                //要将该条数据插入到哪个表中
                int biao=1;
                if(codeMap.get(guZhang)!=null&&systemToBiaoMap.containsKey(codeMap.get(guZhang)))
                    biao=systemToBiaoMap.get(codeMap.get(guZhang));
                String queryBiao="select * from `"+ biao +"` where `code`='"+guZhangAndCheXiang+"'";
                ResultSet rs=null;
                try {
                    pstat=connection.prepareStatement(queryBiao);
                    rs=pstat.executeQuery();

                    StringBuilder sql=new StringBuilder();
                    //有该条数据
                    if(rs.next()){
                        //省略column 1的判断因为肯定有数据
                        System.out.println(rs.getString(1)+" "+rs.getString(2));
                        if (rs.getString(4)==null){
                            sql.append("update `"+biao+"` set `2`='"+temp+"' where code='"+guZhangAndCheXiang+"'");
                        }
                        else if (rs.getString(5)==null){
                            sql.append("update `"+biao+"` set `3`='"+temp+"' where code='"+guZhangAndCheXiang+"'");
                        }
                        else if (rs.getString(6)==null){
                            sql.append("update `"+biao+"` set `4`='"+temp+"' where code='"+guZhangAndCheXiang+"'");
                        }
                        else if (rs.getString(7)==null){
                            sql.append("update `"+biao+"` set `5`='"+temp+"' where code='"+guZhangAndCheXiang+"'");
                        }
                        //如果5个位置都有值了则直接跳过插入以及更新即可
                        else{
                            continue;
                        }

                    }
                    else{//还没有该条数据
                        sql.append("insert into `"+biao+"` (`code`,`1`) values ('"+guZhangAndCheXiang+"','"+temp+"')");
                    }
                    pstat=connection.prepareStatement(sql.toString());
                    pstat.executeUpdate();

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }finally {
                    if ( pstat != null ) {
                        try {
                            pstat.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        /**
         * 释放资源
         * @param context
         */
        public void close(TaskAttemptContext context) throws IOException,InterruptedException{
            if ( connection != null ) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException  {
        return new MySQLRecordWriter();
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException  {

    }

    private FileOutputCommitter committer = null;
    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null: new Path(name);
    }
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
}
