package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @Author: Zhangbao
 * @Date: 14:07 2020/8/17
 * @Description:
 */
public interface DauMapper {
    Integer selectDauTotal(String date);

    List<Map> selectDauTotalHourMap(String date);
}
