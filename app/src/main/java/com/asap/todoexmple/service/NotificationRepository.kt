package com.asap.todoexmple.service

import android.content.Context
import android.util.Log
import com.asap.todoexmple.util.SessionManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext

class NotificationRepository : BaseRepository() {
    // 检查message_id是否存在
    private suspend fun isMessageIdExists(connection: java.sql.Connection, messageId: Int): Boolean {
        val sql = "SELECT COUNT(*) FROM Messages WHERE message_id = ?"
        return connection.prepareStatement(sql).use { stmt ->
            stmt.setInt(1, messageId)
            stmt.executeQuery().use { rs ->
                rs.next() && rs.getInt(1) > 0
            }
        }
    }

    // 使用MessageIdCounter生成新的唯一ID
    private suspend fun generateUniqueMessageId(connection: java.sql.Connection): Int {
        var attempts = 0
        while (attempts < 100) {
            // 使用generateTimestampWithRandom生成新ID
            val newMessageId = generateTimestampWithRandom()
            if (!isMessageIdExists(connection, newMessageId)) {
                Log.d("NotificationRepository", "成功生成新的唯一message_id: $newMessageId")
                return newMessageId
            }
            attempts++
            Log.d("NotificationRepository", "ID冲突，重新生成，尝试次数: $attempts")
        }
        throw Exception("无法生成唯一的message_id，请检查系统状态")
    }

    suspend fun saveNotification(
        context: Context, 
        appName: String?,
        sender: String?, 
        content: String?, 
        messageId: Int
    ): Boolean {
        if (sender == null && content == null) {
            Log.d("NotificationRepository", "标题和内容都为空，跳过保存")
            return false
        }

        val userId = SessionManager.Session.getUserId(context) ?: return false

        return withContext(Dispatchers.IO) {
            var success = false
            for (i in 1..3) { // 最多重试3次
                try {
                    success = executeDbOperation { connection ->
                        // 先检查初始message_id是否可用
                        val uniqueMessageId = if (isMessageIdExists(connection, messageId)) {
                            // 如果存在冲突，重新生成
                            generateUniqueMessageId(connection)
                        } else {
                            messageId
                        }
                        
                        val sql = """
                            INSERT INTO Messages (app_name, sender, content, user_id, message_id, date) 
                            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """
                        connection.prepareStatement(sql).use { stmt ->
                            stmt.setString(1, appName ?: "未知应用")
                            stmt.setString(2, sender ?: "无发送者")
                            stmt.setString(3, content ?: "无内容")
                            stmt.setString(4, userId)
                            stmt.setInt(5, uniqueMessageId)
                            
                            val result = stmt.executeUpdate() > 0
                            if (result) {
                                Log.d("NotificationRepository", "成功插入消息，message_id: $uniqueMessageId")
                            } else {
                                Log.e("NotificationRepository", "插入失败，尝试次数：$i")
                            }
                            result
                        }
                    }
                    if (success) break
                } catch (e: Exception) {
                    Log.e("NotificationRepository", "保存通知出错，尝试次数：$i", e)
                }
                delay(1000L * i) // 延迟重试
            }
            success
        }
    }
}
