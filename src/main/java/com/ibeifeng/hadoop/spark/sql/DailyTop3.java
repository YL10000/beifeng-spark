/*
 * 项目名：beifeng-spark
 * 文件名：DailyTop3.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：每日top3热点搜索词统计案例实战
 * 修改人：yanglin
 * 修改时间：2016年11月19日 下午5:27:31
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

import scala.Tuple2;

/**
 * DailyTop3
 *	
 * @Description 每日top3热点搜索词统计案例实战
 * @author yanglin
 * @version 1.0,2016年11月19日
 * @see
 * @since
 */
public class DailyTop3 {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("DailyTop3").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        HiveContext hiveContext=new HiveContext(context.sc());
        Map<String, List<String>> params=new HashMap<String, List<String>>();
        params.put("citys", Arrays.asList("beijing"));
        params.put("platforms", Arrays.asList("android"));
        params.put("versions", Arrays.asList("1.0","1.2","1.5"));
        final Broadcast<Map<String, List<String>>> paramsBroadcast=context.broadcast(params);
        JavaRDD<String> filtered=context
            //.textFile("hdfs://hadoop-senior.ibeifeng.com:8020/user/yanglin/spark/sql/top3/top.txt")
            .textFile("C://Users//yanglin//Desktop//test//top3//top.txt")
            .filter(new Function<String, Boolean>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Boolean call(String log) throws Exception {
                    String[] logWords=log.split("\t");
                    Map<String, List<String>> condition=paramsBroadcast.getValue();
                    if (condition.containsKey("citys")&&condition.get("citys").size()>0&&!condition.get("citys").contains(logWords[3])) {
                        return false;
                    }
                    if (condition.containsKey("platforms")&&condition.get("platforms").size()>0&&!condition.get("platforms").contains(logWords[4])) {
                        return false;
                    }
                    if (condition.containsKey("versions")&&condition.get("versions").size()>0&&!condition.get("versions").contains(logWords[5])) {
                        return false;
                    }
                    return true;
                }
            });
        JavaRDD<Row> dateWordRdd=filtered.mapToPair(new PairFunction<String, String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(String log) throws Exception {
                String[] logWords=log.split("\t");
                return new Tuple2<String, String>(logWords[0]+"_"+logWords[2], logWords[1]);
            }
        }).groupByKey()
        .mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> tuple)
                    throws Exception {
                List<String> distinctUsers=new ArrayList<String>();
                Iterator<String> users=tuple._2().iterator();
                while (users.hasNext()) {
                    String name=users.next();
                    if(!distinctUsers.contains(name)){
                        distinctUsers.add(name);
                    }
                }
                return new Tuple2<String, Integer>(tuple._1, distinctUsers.size());
            }
        }).map(new Function<Tuple2<String,Integer>, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Tuple2<String, Integer> tuple) throws Exception {
                return RowFactory.create(tuple._1,tuple._2);
            }
        });
        
        
        hiveContext.createDataFrame(dateWordRdd, DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("dateWord", DataTypes.StringType, true),
                    DataTypes.createStructField("uv", DataTypes.IntegerType, true)
                ))).registerTempTable("date_words");
        //select d.dateWord,d.uv from (select dateWord,uv,(row_number() OVER(PARTITION BY dateWord ORDER BY uv DESC)) as rank from date_words ) as d where d.rank < 3
        JavaRDD<Row> topWordsRdd=hiveContext.sql("select d.dateWord,d.uv from (select dateWord,uv,(row_number() OVER(PARTITION BY dateWord ORDER BY uv DESC)) as rank from date_words ) as d where d.rank < 3")
            .toJavaRDD().map(new Function<Row, Row>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Row call(Row row) throws Exception {
                    String[] dateWords=row.getString(0).split("_");
                    return RowFactory.create(dateWords[0],dateWords[1],row.getInt(1));
                }
            });
        
        hiveContext.createDataFrame(topWordsRdd, DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("word", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.IntegerType, true)
            ))).show();
        
        
        
        /*filtered.foreach(new VoidFunction<String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(String log) throws Exception {
                
                System.out.println(log);
            }
        });*/
        //context.close();
    }

}
