package com.gy.canal.handler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.base.CaseFormat;
import com.alibaba.fastjson.JSONObject;
import com.gy.canal.util.MyKafkaSender;
import com.gy.common.constant.GmailConstants;


import java.util.List;

public class CanalHandler {

    public static void handle(String tableName , CanalEntry.EventType eventType , List<CanalEntry.RowData> rowDataList){

        if("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            //下单操作
            for(CanalEntry.RowData rowData : rowDataList){   //行集展开
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for(CanalEntry.Column column : afterColumnsList){
                    System.out.println(column.getName() +"::" + column.getValue());
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(propertyName,column.getValue());
                }
                MyKafkaSender.send(GmailConstants.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }
        }
    }








}
