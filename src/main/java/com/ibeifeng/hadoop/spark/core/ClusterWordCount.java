/*
 * 项目名：beifeng-spark
 * 文件名：WordCount.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：spark实现本地文件的单词计数
 * 修改人：yanglin
 * 修改时间：2016年11月5日 下午5:19:13
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * WordCount
 * 
 * @Description spark实现本地文件的单词计数
 * @author yanglin
 * @version 1.0,2016年11月5日
 * @see
 * @since
 */
public class ClusterWordCount {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .set("/*/pspark.default.parallelism", "5") //设置spark默认并行度
                //设置spark使用的序列化方式kryo serialization，默认使用java.serialization
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //设置kryo序列化时的最大缓存是多少，默认是2M
                .set("spark.kryoSerializer.buffer.mb", "10") 
                //.set("", value)
                .registerKryoClasses(new Class[]{SecondarySortKey.class})//注册kryo可序列化的自定义类型
                .setAppName("ClusterWordCount");
        JavaSparkContext sparkContext=new JavaSparkContext(conf);
        //返回每一行作为一个元素的rdd
        JavaRDD<String> lines=sparkContext.textFile("hdfs://hadoop-senior.ibeifeng.com:8020/user/yanglin/spark/wc.input", 5);//返回为JavaRDD[String]
        //返回每一个单词为一个元素的rdd
        JavaRDD<String> words=lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 2192898403909387476L;

            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
        
        //返回每一个单词的映射
        JavaPairRDD<String, Integer> wordPairs=words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = -4729349390159625475L;

            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        
        //单词数的叠加
        JavaPairRDD<String, Integer> wordCountPairs=wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            

            private static final long serialVersionUID = -8636811420038190538L;

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        
        wordCountPairs.saveAsTextFile("hdfs://hadoop-senior.ibeifeng.com:8020/user/yanglin/spark/sparkWordcountOutput");
        
        /*wordCountPairs.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            
            private static final long serialVersionUID = -8075569726357524136L;

            public void call(Tuple2<String, Integer> pair) throws Exception {
                System.out.println(pair._1+" appread "+ pair._2+" times");
            }
        });*/
        
        sparkContext.close();
        
    }
}
