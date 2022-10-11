package ct.analysis;

import ct.analysis.tool.AnalysisBeanTool;
import ct.analysis.tool.AnalysisTextTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 分析数据
 */
public class AnalysisData {
    public static void main(String[] args) throws Exception {

        int result = ToolRunner.run( new AnalysisTextTool(), args );
//        int result = ToolRunner.run( new AnalysisBeanTool(), args );

    }
}