/*
 * 项目名：beifeng-spark
 * 文件名：AutoPartation.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：自动分区
 * 修改人：yanglin
 * 修改时间：2016年11月15日 下午7:03:12
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.sun.org.apache.bcel.internal.generic.NEW;

/**
 * AutoPartation
 *	
 * @Description 自动分区
 *          设置如下的目录gender=male//country=US//datafromcreate.txt时会把gender和country自动作为分区字段
 * @author yanglin
 * @version 1.0,2016年11月15日
 * @see
 * @since
 */
public class AutoPartation {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("AutoPartation")
                .setMaster("local");
        JavaSparkContext javaSparkContext=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(javaSparkContext);
        //sqlContext.setConf("spark.sql.sources.partitionColumnTypeInference.enabled", "false");
        DataFrame dataFrame=sqlContext.read().format("json")
                .option("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
                .load("C://Users//yanglin//Desktop//test//gender=male//country=US//datafromcreate.txt");
        dataFrame.printSchema();
        dataFrame.show();
        

    }

}
