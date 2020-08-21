package com.atguigu.gmallcanal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.constants.GmallConstants;

import java.util.List;
import java.util.Random;

/**
 * @Author: Zhangbao
 * @Date: 10:03 2020/8/18
 * @Description:
 */
public class CanalHandler {


    private String tableName;
    private CanalEntry.EventType eventType;
    private List<CanalEntry.RowData> rowDataList;

    public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        //判断主题
        if ("order_info".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT)) {
            sendToKafkaTopic(GmallConstants.GMALL_TOPIC_ORDER);
        }else if("user_info".equals(tableName)
                && (eventType.equals(CanalEntry.EventType.INSERT) || eventType.equals(CanalEntry.EventType.UPDATE))) {
            sendToKafkaTopic(GmallConstants.GMALL_TOPIC_USER);
        }else if ("order_detail".equals(tableName)
                && eventType.equals(CanalEntry.EventType.INSERT)) {
            sendToKafkaTopic(GmallConstants.GMALL_TOPIC_ORDER_DETAIL);
        }
    }

    private void sendToKafkaTopic(String topic) {
        //遍历行集
        for (CanalEntry.RowData rowData :
                rowDataList) {
            JSONObject jsonObject = new JSONObject();
            //遍历列集
            for (CanalEntry.Column column :
                    rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            //模拟网络震荡，制造数据延迟
//            try {
//                Thread.sleep(new Random().nextInt(5)*1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }



    /*public static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //判断文件
        if ("order_info".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT)) {
            //遍历行集
            for (CanalEntry.RowData rowData :
                    rowDatasList) {
                JSONObject jsonObject = new JSONObject();
                //遍历列集
                for (CanalEntry.Column column :
                        rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }
                System.out.println(jsonObject.toString());
                MyKafkaSender.send(GmallConstans.GMALL_TOPIC_ORDER, jsonObject.toString());
            }
        } *//*else if ("user_info".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT)) {
            for (CanalEntry.RowData rowData :
                    rowDatasList) {
                JSONObject jsonObject = new JSONObject();
                //遍历列集
                for (CanalEntry.Column column :
                        rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }
                MyKafkaSender.send(GmallConstance.GMALL_TOPIC_USER, jsonObject.toString());
            }
        }*//*
    }*/
}
