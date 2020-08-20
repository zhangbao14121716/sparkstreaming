package com.atguigu.gmallpublisher.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @GetMapping("realtime-total")
    public String realtimeHourDate(@RequestParam("date") String date) {

        //1.查询日活总数
        Integer dauTotal = publisherService.getDauTotal(date);
        //1.1查询新增总数
        Integer newMidTotal = publisherService.getNewMidTotal(date);
        //1.2查询订单总金额
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);

        //2.创建List用于存放结果数据
        List<Map> resultList = new ArrayList<>();

        //3.创建Map用于存放日活数据
        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "日活跃数");
        dauMap.put("value", dauTotal);
        //3.1创建Map用于存放新增用户数据
        Map<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", newMidTotal);
        //3.2创建Map用于存放总金额
        Map<String, Object> orderAmountMap = new HashMap<>();
        orderAmountMap.put("id", "order_amount");
        orderAmountMap.put("name", "订单总额");
        orderAmountMap.put("value", orderAmountTotal);

        //4.将新增Map存放在list中
        resultList.add(dauMap);
        resultList.add(newMidMap);
        resultList.add(orderAmountMap);
        //5.返回结果
        return JSONObject.toJSONString(resultList);
    }

    @GetMapping("realtime-hours")
    public String realtimeHourDate(@RequestParam("id") String id, @RequestParam("date") String date) {
        HashMap<String, Object> result = new HashMap<>();
        JSONObject jsonObject = new JSONObject();
        String yesterdayDateString = LocalDate.parse(date).plusDays(-1).toString();
        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            todayMap = publisherService.getDauHours(date);
            yesterdayMap = publisherService.getDauHours(yesterdayDateString);

        } else if ("new_mid".equals(id)) {
            yesterdayMap = new HashMap<>();
            yesterdayMap.put("09", 100L);
            yesterdayMap.put("12", 200L);
            yesterdayMap.put("17", 150L);

            todayMap = new HashMap<>();
            todayMap.put("10", 400L);
            todayMap.put("13", 450L);
            todayMap.put("15", 500L);
            todayMap.put("20", 600L);
        } else if ("order_amount".equals(id)) {
            todayMap = publisherService.getOrderAmountHours(date);
            yesterdayMap = publisherService.getOrderAmountHours(yesterdayDateString);
        }
        result.put("today", todayMap);
        result.put("yesterday", yesterdayMap);
        return JSONObject.toJSONString(result);
    }

/*    @GetMapping("realtime-hours1")
    public String realtimeHourDate1(@RequestParam("id") String id, @RequestParam("date") String date) {
        //1.
        if ("dau".equals(id)) {
            //1.获取今天的数据
            Map<String, Long> dauHoursToday = publisherService.getDauHours(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today", dauHoursToday);
            //2.获取昨天的数据
            String yesterdayDateString = LocalDate.parse(date).plusDays(-1).toString();
            Map<String, Long> dauHoursYesterday = publisherService.getDauHours(yesterdayDateString);
            jsonObject.put("yesterday", dauHoursYesterday);
            //3.返回昨天，今天的一起的json对象的字符串
            return jsonObject.toString();
        }else if ("new_mid".equals(id)) {
            return publisherService.getNewMidHours(date).toString();
        }else if ("order_amount".equals(id)) {
            //1.获取今天数据
            Map<String, Double> orderAmountHours = publisherService.getOrderAmountHours(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today",orderAmountHours);

            String yesterdayDateString = LocalDate.parse(date).plusDays(-1).toString();
            Map orderAmountHoursYesterday = publisherService.getOrderAmountHours(yesterdayDateString);
            jsonObject.put("yesterday", orderAmountHoursYesterday);
            return jsonObject.toString();
        }


        return null;
    }*/
}