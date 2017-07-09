/*
 * 项目名：beifeng-spark
 * 文件名：InnerFunction.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：内置函数
 * 修改人：yanglin
 * 修改时间：2016年11月17日 下午5:40:56
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.CountDistinct;
import org.apache.spark.sql.catalyst.expressions.Max;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions;

import groovy.swing.factory.ColumnFactory;

/**
 * InnerFunction
 *	
 * @Description 内置函数
 * @author yanglin
 * @version 1.0,2016年11月17日
 * @see
 * @since
 */
public class InnerFunction {
    
    private static List<String> logs=Arrays.asList(
            "2016-11-10 1234 56.26",
            "2016-11-11 1235 26.30",
            "2016-11-10 1236 30.02",
            "2016-11-10 1234 26.10",
            "2016-11-11 1234 15.26",
            "2016-11-12 1236 36.25",
            "2016-11-10 1235 28.26"
            );
    

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("InnerFunction").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        JavaRDD<Row> logRdd=context.parallelize(logs).map(new Function<String, Row>() {

            private static final long serialVersionUID = 1L;

            public Row call(String log) throws Exception {
                String[] logWords=log.split(" ");
                return RowFactory.create(logWords[0],logWords[1],logWords[2]);
            }
        });
        
        sqlContext.createDataFrame(logRdd, DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date",DataTypes.StringType, true),
                DataTypes.createStructField("id",DataTypes.IntegerType, true),
                DataTypes.createStructField("price",DataTypes.DoubleType, true)
                ))).groupBy("date").count().show();
    }

}
