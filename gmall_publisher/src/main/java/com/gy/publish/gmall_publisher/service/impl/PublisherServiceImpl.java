package com.gy.publish.gmall_publisher.service.impl;

import com.gy.common.constant.GmailConstants;
import com.gy.publish.gmall_publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmailConstants.ES_INDEX_DAU).addType("_doc").build();
        Long count = null;

        try {
            SearchResult result = jestClient.execute(search);
            count = result.getTotal();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    @Override
    public Map<String, String> getDauHourMap(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合
        TermsAggregationBuilder aggsBuildr = AggregationBuilders.terms("groupby_logHour").field("logHour.keyword").size(24);
        searchSourceBuilder.aggregation(aggsBuildr);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmailConstants.ES_INDEX_DAU).addType("_doc").build();
        Map<String,String> dauHourMap = new HashMap<>();

        try {
            SearchResult result = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = result.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for(TermsAggregation.Entry bucket : buckets){
                String key = bucket.getKey();
                Long count = bucket.getCount();
                dauHourMap.put(key,count.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dauHourMap;
    }
}
