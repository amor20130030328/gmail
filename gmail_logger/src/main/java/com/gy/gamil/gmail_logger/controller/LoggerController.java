package com.gy.gamil.gmail_logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import com.gy.common.constant.GmailConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {


    private Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("/log")
    public String doLog(@RequestParam("log")String log){

        //补时间戳
        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("ts",System.currentTimeMillis());
        logger.info(jsonObject.toJSONString());
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmailConstants.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        }else{
            kafkaTemplate.send(GmailConstants.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }

        return log;
    }


}
