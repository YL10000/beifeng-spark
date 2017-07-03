/*
 * 项目名：beifeng-spark
 * 文件名：UDFFunction.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：spark sql中使用自定义的udf函数
 * 修改人：yanglin
 * 修改时间：2016年11月18日 下午8:00:27
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * UDFFunction
 *	
 * @Description spark sql中使用自定义的udf函数
 * @author yanglin
 * @version 1.0,2016年11月18日
 * @see
 * @since
 */
public class UDFFunction {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("UDFFunction").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        sqlContext.read().json("C://Users//yanglin//Desktop//test//scores.txt").registerTempTable("scores");;
        //使用java8中的lambda表达式创建匿名函数
        //sqlContext.udf().register("strLen", (String s)->s.length(),DataTypes.IntegerType);
        //sqlContext.sql("select name,strLen(name) from scores").show();
        context.close();
        
    }

}
