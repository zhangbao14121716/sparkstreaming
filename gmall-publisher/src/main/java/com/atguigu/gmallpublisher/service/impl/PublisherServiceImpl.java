package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private JestClient jestClient;

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
            dauHourMap.put(dauHour.get("LH"), dauHour.get("CT"));
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

    @Override
    public Map getSaleDetail(String date, int startPage, int size, String keyword) {
        //创建查询源
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //a.条件查询
        //a.2
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //a.4
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt", date);
        //a.3
        boolQueryBuilder.filter(termQueryBuilder);
        //a.6
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND);
        //a.5
        boolQueryBuilder.must(matchQueryBuilder);
        //a.1
        searchSourceBuilder.query(boolQueryBuilder);
        //b.聚合查询
        //b.2
        TermsBuilder ageBuilder = AggregationBuilders.terms("groupBy_age")
                .field("user_age")
                .size(100);
        TermsBuilder genderBuilder = AggregationBuilders.terms("groupBy_gender")
                .field("user_gender")
                .size(3);
        //b.1
        searchSourceBuilder.aggregation(ageBuilder);
        searchSourceBuilder.aggregation(genderBuilder);
        //c.分页查询
        //行号=（页号-1）*页面大小
        searchSourceBuilder.from((startPage - 1) * size);
        searchSourceBuilder.size(size);


        //封装查询语句
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(GmallConstants.GMALL_ES_SALE_DETAIL_PRE + "-query")
                .addType("_doc")
                .build();
        //执行查询任务
        SearchResult searchResult = null;
        try {
            searchResult = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //对查询结果进行解析
        //创建Map[]
        HashMap<String, Object> resultMap = new HashMap<>();
        //a.获取总数
        Long total = searchResult.getTotal();
        //b.处理聚合数据
        //获取所有聚合组
        MetricAggregation aggregations = searchResult.getAggregations();
        //存放年龄比例，或者性别比例
        ArrayList<Stat> statArrayList = new ArrayList<>();
        //处理年龄数据
        Stat ageStat = new Stat();
        List<TermsAggregation.Entry> ageBuckets = aggregations.getTermsAggregation("groupBy_age").getBuckets();
        Long lower25 = 0L;
        Long upper25to30 = 0L;
        for (TermsAggregation.Entry ageBucket :
                ageBuckets) {
            int age = Integer.parseInt(ageBucket.getKey());
            if (age < 25) {
                lower25 += ageBucket.getCount();
            } else if (age < 30) {
                upper25to30 += ageBucket.getCount();
            }
        }
        double lower25Rate = Math.round(lower25 * 1000D / total) / 10D;
        double upper25to30Rate = Math.round(upper25to30 * 1000D / total) / 10D;
        double upper30Rate = 100 - lower25Rate - upper25to30Rate;
        ArrayList<Option> optionAgeArrayList = new ArrayList<>();
        optionAgeArrayList.add(new Option("25岁以下", lower25Rate));
        optionAgeArrayList.add(new Option("25-30岁", upper25to30Rate));
        optionAgeArrayList.add(new Option("30岁以上", upper30Rate));
        ageStat.setTitle("年龄比例");
        ageStat.setOptions(optionAgeArrayList);
        statArrayList.add(ageStat);

        //处理性别数据
        Stat genderStat = new Stat();
        Long numFemale = 0L;
        List<TermsAggregation.Entry> genderBuckets = aggregations.getTermsAggregation("groupBy_gender").getBuckets();
        for (TermsAggregation.Entry genderBucket :
                genderBuckets) {
            if (genderBucket.getKey().equals("F")) {
                numFemale += genderBucket.getCount();
            }
        }
        double femaleRate = Math.round(numFemale * 1000D / total) / 10D;
        double maleRate = 100 - femaleRate;
        ArrayList<Option> optionGenderArrayList = new ArrayList<>();
        optionGenderArrayList.add(new Option("女", femaleRate));
        optionGenderArrayList.add(new Option("男", maleRate));
        genderStat.setTitle("性别比例");
        genderStat.setOptions(optionGenderArrayList);
        statArrayList.add(genderStat);
        //c.详情数据
        ArrayList<Map> saleDetailList = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> resultHits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit :
        resultHits) {
            saleDetailList.add(hit.source);
        }

        //将所有数据放入结果map中
        resultMap.put("total", total);
        resultMap.put("stat", statArrayList);
        resultMap.put("detail",saleDetailList);
        //！！！！！！！！！！！!不用关闭连接池
        return resultMap;
    }
}