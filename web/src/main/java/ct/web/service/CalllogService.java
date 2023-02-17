package ct.web.service;

import ct.web.bean.Calllog;

import java.util.List;

public interface CalllogService {
    List<Calllog> queryMonthDatas(String tel, String calltime);
}
