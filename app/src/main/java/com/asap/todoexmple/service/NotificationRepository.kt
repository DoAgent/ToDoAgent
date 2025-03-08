package com.asap.todoexmple.service

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import com.asap.todoexmple.util.DatabaseHelper


class NotificationRepository {
    suspend fun saveNotification(sender: String?, content: String?): Boolean = withContext(Dispatchers.IO) {
        var connection: java.sql.Connection? = null

        try {
            if (sender == null && content == null) {
                Log.d("NotificationRepository", "标题和内容都为空，跳过保存")
                return@withContext false
            }

            connection = DatabaseHelper.getConnection()

            val sql = "INSERT INTO Messages (sender, content) VALUES (?, ?)"
            connection?.prepareStatement(sql)?.use { stmt ->
                stmt.setString(1, sender ?: "无标题")
                stmt.setString(2, content ?: "无内容")

                Log.d("NotificationRepository", "准备保存通知: sender=$sender, content=$content")
                return@withContext stmt.executeUpdate() > 0
            }
            false
        } catch (e: Exception) {
            Log.e("NotificationRepository", "保存通知失败: ${e.message}")
            e.printStackTrace()
            false
        } finally {
            try {
                connection?.close()
            } catch (e: Exception) {
                Log.e("NotificationRepository", "关闭连接失败", e)
            }
        }
    }
}
