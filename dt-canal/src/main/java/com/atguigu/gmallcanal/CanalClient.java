package com.atguigu.gmallcanal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

/**
 * @Author: Zhangbao
 * @Date: 10:03 2020/8/18
 * @Description:
 */
public class CanalClient {

    public static void main(String[] args) {
        //1.获取canal连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop103", 11111), "example", "", "");
        //2.
        while (true) {

            //a.连接
            canalConnector.connect();
            //b.订阅监控的表
            canalConnector.subscribe("gmall0317.*");
            //c.抓取数据
            Message message = canalConnector.get(100);
            //d.判断当前是否抓取到数据
            if (message.getEntries().size() <= 0) {
                System.out.println("没抓到数据");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //e.1.如果抓取到数据对message结构进行解析
                for (CanalEntry.Entry entry :
                        message.getEntries()) {
                    //e.2.获取行数据集，先判断是否是行数据集的类型
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        //e.3获取表名
                        String tableName = entry.getHeader().getTableName();
                        //e.4获取数据, 对二进制数据进行反序列化
                        ByteString storeValue = entry.getStoreValue();
                        try {
                            //e.5用rowchange反序列化
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //e.6获取RowDataList的集合
                            List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                            //e.7获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //e.8处理数据
                            (new CanalHandler(tableName, eventType, rowDataList)).handle();
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

}
