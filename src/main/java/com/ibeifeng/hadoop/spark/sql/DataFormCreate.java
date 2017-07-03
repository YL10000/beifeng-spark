/*
 * 项目名：beifeng-spark
 * 文件名：DataFormCreate.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：datafrom的创建
 * 修改人：yanglin
 * 修改时间：2016年11月14日 下午7:13:09
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SQLContext;

/**
 * DataFormCreate
 *	
 * @Description datafrom的创建
 * @author yanglin
 * @version 1.0,2016年11月14日
 * @see
 * @since
 */
public class DataFormCreate {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("DataFormCreate")
                .setMaster("local");
        
        JavaSparkContext context=new JavaSparkContext(conf);
        //创建sqlContext对象
        SQLContext sqlContext=new SQLContext(context);
        //加载json文件
        //sqlContext.read().json("C://Users//yanglin//Desktop//test//datafromcreate.txt").show();
        sqlContext.read().format("json").load("C://Users//yanglin//Desktop//test//datafromcreate.txt")
            .select(new Column("name"),new Column("age")).write().save("C://Users//yanglin//Desktop//test//nameAndAge.txt");;
        //.show();
    }

}
