package com.gy.publish.gmall_publisher.controller;

import com.alibaba.fastjson.JSON;
import com.gy.publish.gmall_publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PubisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("/realtime-total")
    public String getTotal(@RequestParam("date") String date){
        List<Map<String,String>> result = new ArrayList<>();
        Map<String,String> dauMap = new HashMap<>();
        Long dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal.toString());
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        result.add(dauMap);
        Map<String,String> newMap = new HashMap<>();
        newMap.put("value","231");
        newMap.put("id","newMid");
        newMap.put("name","新增设备");
        result.add(newMap);
        return JSON.toJSONString(result);
    }

    @GetMapping("/realtime-hours")
    public String getDauHourMap(@RequestParam("id") String id ,@RequestParam("date") String date){

        if("dau".equals(id)){
            //今天
            Map<String, String> dateHoutTDMap = publisherService.getDauHourMap(date);
            String yesterday = getYesterday(date);
            Map<String, String> dateHoutYDMap = publisherService.getDauHourMap(yesterday);
            Map hourMap=new HashMap();
            hourMap.put("today",dateHoutTDMap);
            hourMap.put("yesterday",dateHoutYDMap);
            return  JSON.toJSONString(hourMap);
        }
        return  null;

    }

    private String getYesterday(String today){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = "";
        try {
            Date todayDate = simpleDateFormat.parse(today);
            Date yesterdayDate = DateUtils.addDays(todayDate,-1);
            yesterday = simpleDateFormat.format(yesterdayDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterday;
    }
}
