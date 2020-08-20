package com.atguigu.gmallcanal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

/**
 * @Author: Zhangbao
 * @Date: 10:03 2020/8/18
 * @Description:
 */
public class CanalHandler {

    public static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
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
                MyKafkaSender.send(GmallConstance.GMALL_TOPIC_ORDER, jsonObject.toString());
            }
        } /*else if ("user_info".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT)) {
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
        }*/
    }
}
