package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {
    //获取日活总数
    Integer getDauTotal(String date);
    //获取分时日活总数
    Map<String, Long> getDauHours(String date);
    //获取新增设备总数
    Integer getNewMidTotal(String date);
    //
    Integer getNewMidHours(String date);
    //获取订单总数
    Double getOrderAmountTotal(String date);
    //分时获取订单总额
    Map getOrderAmountHours(String date);
    //
    Map getSaleDetail(String date, int startPage, int size, String keyword) throws IOException;

}