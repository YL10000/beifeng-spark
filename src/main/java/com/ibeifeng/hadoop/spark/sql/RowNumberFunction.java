/*
 * 项目名：beifeng-spark
 * 文件名：RowNumberFunction.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：row_number函数的使用
 * 修改人：yanglin
 * 修改时间：2016年11月18日 下午2:01:02
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * RowNumberFunction
 *	
 * @Description row_number函数的使用
 * @author yanglin
 * @version 1.0,2016年11月18日
 * @see
 * @since
 */
public class RowNumberFunction {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("RowNumberFunction");
        JavaSparkContext context=new JavaSparkContext(conf);
        HiveContext hiveContext=new HiveContext(context.sc());
        
        //删除表
        hiveContext.sql("DROP TABLE IF EXISTS sqark.sales");
        //创建表
        hiveContext.sql("CREATE TABLE IF NOT EXISTS sqark.sales (name string,category string,price int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
        
        //加载数据
        hiveContext.sql("LOAD DATA INPATH '/user/yanglin/spark/sql/sales/input/sales.txt' OVERWRITE INTO TABLE sqark.sales");
        
        //分组后查找topn
        hiveContext.sql("select t.name,t.category,t.price from (select name,category,price,row_number() OVER(PARTITION BY category ORDER BY price DESC) as rank from sqark.sales) as t  where t.rank <=3")
            .show();
    }
}
