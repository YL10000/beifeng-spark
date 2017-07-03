/*
 * 项目名：beifeng-spark
 * 文件名：StudentScoreHive.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：根据学生成绩查找学生信息hive版本
 * 修改人：yanglin
 * 修改时间：2016年11月17日 下午1:42:16
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * StudentScoreHive
 *	
 * @Description 根据学生成绩查找学生信息hive版本
 * @author yanglin
 * @version 1.0,2016年11月17日
 * @see
 * @since
 */
public class StudentScoreHive {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("StudentScoreHive");
        JavaSparkContext context=new JavaSparkContext(conf);
        //使用sparkContext,创建hivecontext对象，
        HiveContext hiveContext=new HiveContext(context.sc());
        
        
        //如果表sqark.student_info存在就进行删除
        hiveContext.sql("DROP TABLE IF EXISTS sqark.student_info");
        //创建表sqark.student_info
        hiveContext.sql("CREATE TABLE IF NOT EXISTS sqark.student_info (name string,age int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '");
        //加载数据到表sqark.student_info
        hiveContext.sql("LOAD DATA INPATH '/user/yanglin/spark/sql/score/hive/student_info.txt' OVERWRITE INTO TABLE sqark.student_info");
        
        hiveContext.sql("DROP TABLE IF EXISTS sqark.student_score");
        //导入表数据sqark.student_score
        hiveContext.sql("CREATE TABLE IF NOT EXISTS sqark.student_score (name string,score int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '");
        //加载数据到表sqark.student_score
        hiveContext.sql("LOAD DATA INPATH '/user/yanglin/spark/sql/score/hive/student_score.txt' OVERWRITE INTO TABLE sqark.student_score");
        
        //将结果保存到表sqark.good_student_info
        hiveContext.sql("DROP TABLE IF EXISTS sqark.good_student_info");
        hiveContext.sql("select s.name,s.score,i.age from sqark.student_score s left join sqark.student_info i on s.name=i.name where s.score>80")
            .saveAsTable("sqark.good_student_info");
        
        hiveContext.table("sqark.good_student_info").show();
        context.close();
    }

}
