package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHours(String date) {
        HashMap dauHourMap = new HashMap<>();
        List<Map> dauToHourList = dauMapper.selectDauTotalHourMap(date);
        for (Map dauHour :
                dauToHourList) {
            dauHourMap.put(dauHour.get("LH"),dauHour.get("CT"));
        }

        return dauHourMap;
    }

    @Override
    public Integer getNewMidTotal(String date) {
        return 100;
    }

    @Override
    public Integer getNewMidHours(String date) {
        return 50;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHours(String date) {
/*        HashMap<String, Double> orderAmountHourMap = new HashMap<>();
        List<Map> orderAmountHourList = orderMapper.selectOrderAmountHourMap(date);
        for (Map orderAmountHour :
                orderAmountHourList) {
            orderAmountHourMap.put((String) orderAmountHour.get("CREATE_HOUR"), (Double) orderAmountHour.get("SUM_AMOUNT"));
        }
        return orderAmountHourMap;*/
        //1.查询Phoenix
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建Map用于存放结果数据
        HashMap<String, Double> result = new HashMap<>();

        //3.遍历list,调整结构
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        //4.返回结果
        return result;
    }
}