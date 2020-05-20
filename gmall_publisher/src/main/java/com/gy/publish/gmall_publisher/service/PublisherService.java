package com.gy.publish.gmall_publisher.service;


import java.util.Map;

public interface PublisherService {

    public Long getDauTotal(String date);

    public Map<String,String> getDauHourMap(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHourMap(String date);
}
