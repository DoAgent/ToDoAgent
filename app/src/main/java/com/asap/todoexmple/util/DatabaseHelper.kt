package com.asap.todoexmple.util

import android.util.Log
import java.sql.Connection
import java.sql.DriverManager

class DatabaseHelper {
    companion object {
        private const val HOST = "103.116.245.150"
        private const val PORT = "3306"
        private const val DATABASE = "ToDoAgent" // 替换为您的数据库名
        private const val USERNAME = "root"
        private const val PASSWORD = "4bc6bc963e6d8443453676"

        fun getConnection(): Connection? {
            return try {
                Class.forName("com.mysql.jdbc.Driver")
                val url = "jdbc:mysql://$HOST:$PORT/$DATABASE?useSSL=false&"+
                        "useUnicode=true&"+
                        "characterEncoding=utf8&" +
                        "characterSetResults=utf8"
                DriverManager.getConnection(url, USERNAME, PASSWORD)
            } catch (e: Exception) {
                Log.e("DatabaseHelper", "数据库连接失败: ${e.message}")
                e.printStackTrace()
                null
            }
        }
    }
}
