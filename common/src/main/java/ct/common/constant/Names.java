package ct.common.constant;

import ct.common.bean.Val;

/**
 * 名称常量枚举类
 */
public enum Names implements Val {
    NAMESPACE("ct")//命名空间
    ,TABLE("ct:tky")//表名
    ,CF_INFO("info")//表的列族
    ,TOPIC("ct");//kafka的主题


    private String name;

    private Names( String name ) {
        this.name = name;
    }


    public void setValue(Object val) {
       this.name = (String)val;
    }

    public String getValue() {
        return name;
    }
}
