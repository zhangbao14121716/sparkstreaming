package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @Author: Zhangbao
 * @Date: 18:37 2020/8/18
 * @Description:
 */
public interface OrderMapper {
    //查询新增总金额
    Double selectOrderAmountTotal(String date);
    //查询分时总金额
    List<Map> selectOrderAmountHourMap(String date);

}
