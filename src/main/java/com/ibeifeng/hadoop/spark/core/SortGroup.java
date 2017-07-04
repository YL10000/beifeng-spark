/*
 * 项目名：beifeng-spark
 * 文件名：SortGroup.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：分组后排序
 * 修改人：yanglin
 * 修改时间：2016年11月8日 下午1:22:52
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import jodd.util.collection.SortedArrayList;
import scala.Tuple2;

/**
 * SortGroup
 *	
 * @Description 分组后排序取topN
 *      [(class1,[98, 87, 78]), (class2,[94, 85, 79])]
 * @author yanglin
 * @version 1.0,2016年11月8日
 * @see
 * @since
 */
public class SortGroup {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("SortGroup").setMaster("local");
        final JavaSparkContext context=new JavaSparkContext(conf);
        List<Tuple2<String, List<Integer>>> result=context.textFile("src/main/resources/group.txt")
            .mapToPair(new PairFunction<String, String, Integer>() {

                private static final long serialVersionUID = 1L;

                public Tuple2<String, Integer> call(String v) throws Exception {
                    return new Tuple2<String, Integer>(v.split(" ")[0], Integer.parseInt(v.split(" ")[1]));
                }
            })
            //按key进行分组
            .groupByKey()
            .mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, List<Integer>>() {

                private static final long serialVersionUID = 1L;

                public Tuple2<String, List<Integer>> call(
                        Tuple2<String, Iterable<Integer>> s) throws Exception {
                    String className=s._1;
                    Integer[] topScores=new Integer[3];
                    Iterator<Integer> iterator=s._2.iterator();
                    //使用SortedArrayList对集合中的数据进行自动排序（升序）
                    List<Integer> list=new SortedArrayList<>();
                    while (iterator.hasNext()) {
                        list.add(iterator.next());
                    }
                    
                    //取集合中升序排序后的最后N个作为topN的值
                    for(int i=0;i<topScores.length;i++){
                        topScores[i]=list.get(list.size()-1-i);
                    }
                    return new Tuple2<String, List<Integer>>(className, Arrays.asList(topScores));
                }
            }).collect();
        
        //[(class1,[98, 87, 78]), (class2,[94, 85, 79])]
        System.out.println(result);
        context.close();
    }
}
