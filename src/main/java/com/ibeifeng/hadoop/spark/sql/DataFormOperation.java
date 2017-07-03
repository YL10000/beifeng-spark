/*
 * 项目名：beifeng-spark
 * 文件名：DataFormOperation.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：datafrom操作
 * 修改人：yanglin
 * 修改时间：2016年11月14日 下午7:46:25
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * DataFormOperation
 *	
 * @Description datafrom操作
 * @author yanglin
 * @version 1.0,2016年11月14日
 * @see
 * @since
 */
public class DataFormOperation {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("DataFormOperation")
                .setMaster("local");
        
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        DataFrame dataFrame=sqlContext.read().json("C://Users//yanglin//Desktop//test//datafromcreate.txt");
        //查询所有数据
        dataFrame.show();
        
        //打印元数据
        dataFrame.printSchema();
        
        Column column=dataFrame.col("name");
        
        //查询表中的指定列
        dataFrame.select(dataFrame.col("name"));
        
        //查询指定列，并对指定的列进行操作
        dataFrame.select(dataFrame.col("name"),dataFrame.col("age").$plus(1)).show();
        
        //查询符合指定条件的数据
        dataFrame.filter(dataFrame.col("age").gt(30)).show();
        
        //分组后统计个数
        dataFrame.groupBy(dataFrame.col("name")).count().show();
    }

}
