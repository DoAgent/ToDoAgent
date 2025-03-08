package com.asap.todoexmple.application


import android.util.Log

import com.asap.todoexmple.util.DatabaseHelper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext



class SmsRepository {
    suspend fun saveSmsData(sender: String, content: String): Boolean = withContext(Dispatchers.IO) {
        var connection: java.sql.Connection? = null

        try {
            connection = DatabaseHelper.getConnection()

            // 检查字符集设置
            connection?.createStatement()?.executeQuery("SHOW VARIABLES LIKE 'character%'")?.use { rs ->
                while (rs.next()) {
                    Log.d("SmsRepository", "${rs.getString(1)}: ${rs.getString(2)}")
                }
            }

            val sql = "INSERT INTO Messages (sender, content) VALUES (?, ?)"
            connection?.prepareStatement(sql)?.use { stmt ->
                stmt.setString(1, sender)
                stmt.setString(2, content)
                //stmt.setString(3, timestamp)

                // 打印实际插入的值
                Log.d("SmsRepository", "准备插入: sender=$sender, content=$content")

                return@withContext stmt.executeUpdate() > 0
            }
            false
        } catch (e: Exception) {
            Log.e("SmsRepository", "保存失败: ${e.message}")
            e.printStackTrace()
            false
        } finally {
            connection?.close()
        }
    }
}

