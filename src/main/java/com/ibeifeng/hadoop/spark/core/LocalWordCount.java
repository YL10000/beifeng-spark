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

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

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
public class LocalWordCount {
    
    static SparkConf conf = new SparkConf().setAppName("LocalWordCount").setMaster("local");// 指定运行在本地
    static JavaSparkContext sparkContext = new JavaSparkContext(conf);

    public static void main(String[] args) {
        
        /**
         * 读取同一个目录下下的文件，返回每行为元素的RDD
         */
        // 返回每一行作为一个元素的rdd
        JavaRDD<String> lines = sparkContext
                .textFile("src/main/resources/wordcount.txt", 5); // 返回为JavaRDD[String]
                // .cache()
                //.persist(StorageLevel.MEMORY_ONLY_SER());
        
        
        /**
         * 读取不同目录下的文件，返回<fileName,content>的RDD，仅适用于小文档
         * 
         * [(file:/D:/workhome/workhome2/beifeng-spark/src/main/resources/wordcount.txt,
         *      hello scala java
                hello spark
                hello yarn
                hadoop spark yarn), 
             (file:/D:/workhome/workhome2/beifeng-spark/src/main/resources/other/aa.txt,
                 hadoop yarn
                scale java)]
         */
        JavaPairRDD<String, String> files=sparkContext.wholeTextFiles("src/main/resources/wordcount.txt,src/main/resources/other/aa.txt");
       
        //返回以每个文档为元素的RDD
        JavaRDD<String> textRdd=files.map(new Function<Tuple2<String,String>, String>() {

            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                //将文档中的换行符替换为空格，将回车符去掉
                return v1._2.replaceAll("\r"," ").replaceAll("\n", "");
            }
        });
        
        //返回以每个单词为元素的RDD
        JavaRDD<String> words=textRdd.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String text) throws Exception {
                return Arrays.asList(text.split(" "));
            }
        });
        
        System.out.println(textRdd.collect());
        
        
        
        
        
        // 返回每一个单词为一个元素的rdd
        /*JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 2192898403909387476L;

            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });*/

        // 返回每一个单词的映射
        JavaPairRDD<String, Integer> wordPairs = words
                .mapToPair(new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = -4729349390159625475L;

                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });

        // 单词数的叠加
        JavaPairRDD<String, Integer> wordCountPairs = wordPairs
                .reduceByKey(new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = -8636811420038190538L;

                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
        
        //将单词按出现的次数倒序排列
        wordCountPairs = wordCountPairs
                //将key和value进行翻转
                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<Integer, String> call(Tuple2<String, Integer> v)
                            throws Exception {
                        return new Tuple2<Integer, String>(v._2, v._1);
                    }
                })
               //按key进行倒序排列
                .sortByKey(false)
                //将key和value进行翻转
                .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<String, Integer> call(Tuple2<Integer, String> v)
                            throws Exception {
                        return new Tuple2<String, Integer>(v._2, v._1);
                    }
                });

        List<Tuple2<String, Integer>> result=wordCountPairs.collect();
        System.out.println(result);
        /*wordCountPairs.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            private static final long serialVersionUID = -8075569726357524136L;

            public void call(Tuple2<String, Integer> pair) throws Exception {
                System.out.println(pair._1 + " appread " + pair._2 + " times");
            }
        });*/

        sparkContext.close();

    }
    
}
