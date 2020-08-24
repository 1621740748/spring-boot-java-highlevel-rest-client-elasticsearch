package com.example.aws.elasticsearch.demo;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationTests {
	@Autowired
    private RestHighLevelClient client;
	@Autowired
    private ObjectMapper objectMapper;
	@Test
	public void contextLoads() {
	}
	@Test
	public void test1() {
		SearchRequest searchRequest = new SearchRequest();
	       SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
	       TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_technology")
	               .field("technologies.name.keyword");   //text类型不能用于索引或排序，必须转成keyword类型
	       aggregation.subAggregation(AggregationBuilders.count("tCount")
	               .field("technologies.name.keyword"));  //avg_age 为子聚合名称，名称可随意
	       searchSourceBuilder.aggregation(aggregation);
	       searchRequest.source(searchSourceBuilder);
	       SearchResponse searchResponse = null;
	       try {
	           searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
	       } catch (IOException e) {
	           e.printStackTrace();
	       }
	       Aggregations aggregations = searchResponse.getAggregations();

	       Terms byCompanyAggregation = aggregations.get("by_technology");
	       Terms.Bucket elasticBucket = byCompanyAggregation.getBucketByKey("java");
//	       try {
//			System.out.println(objectMapper.writeValueAsString(elasticBucket));
//		} catch (JsonProcessingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	       ValueCount averageAge = elasticBucket.getAggregations().get("tCount");
	       double avg = averageAge.getValue();
	       System.out.println("java 人数："+avg);
	}
}
