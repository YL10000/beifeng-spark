/*
 * 项目名：beifeng-spark
 * 文件名：SecondarySort.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：二次排序
 * 修改人：yanglin
 * 修改时间：2016年11月7日 下午6:58:47
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.core;


import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * SecondarySort
 *	
 * @Description 二次排序
 * @author yanglin
 * @version 1.0,2016年11月7日
 * @see
 * @since
 */
public class SecondarySort {

    public static void main(String[] args) {
        JavaSparkContext context=TransactionOperation.getSparkContext();
        List<String> lines=context.textFile("src/main/resources/sort.txt")
        .mapToPair(new PairFunction<String, SecondarySortKey, String>() {

            private static final long serialVersionUID = 1L;

            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                return new Tuple2<SecondarySortKey, String>(new SecondarySortKey(Integer.parseInt(line.split(" ")[0]), Integer.parseInt(line.split(" ")[1])), line);
            }
        })
        .sortByKey(false)
        .map(new Function<Tuple2<SecondarySortKey,String>, String>() {

            private static final long serialVersionUID = 1L;

            public String call(Tuple2<SecondarySortKey, String> v) throws Exception {
                return v._2;
            }
        })
        .collect();
        System.out.println(lines);
        /*.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 1L;
            
            public void call(String v) throws Exception {
                System.out.println(v);
            }
        });*/
        
        TransactionOperation.closeSparkContext(context);
    }
}
