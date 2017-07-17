/*
 * 项目名：beifeng-spark
 * 文件名：ConnectionPool.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：自定义mysql连接池
 * 修改人：yanglin
 * 修改时间：2017年7月17日 上午7:25:22
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;


import kafka.consumer.StaticTopicCount;

/**
 * ConnectionPool
 *	
 * @Description 自定义mysql连接池
 * @author yanglin
 * @version 1.0,2017年7月17日
 * @see
 * @since
 */
public class ConnectionPool {

    private static LinkedList<Connection>  connectionQueue;
    
    static{
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    public synchronized static Connection  getConnection() {
        if (connectionQueue==null) {
            connectionQueue=new LinkedList<Connection>();
            for(int i=0;i<10;i++){
                try {
                    Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop-senior.ibeifeng.com:3306/spark", "root", "123456");
                    connectionQueue.push(connection);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return connectionQueue.poll();
    }
    
    public static void  returnConnection(Connection connection) {
        connectionQueue.push(connection);
    }
    
}
