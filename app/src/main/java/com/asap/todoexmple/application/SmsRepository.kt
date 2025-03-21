package com.asap.todoexmple.application

import android.content.Context
import android.util.Log
import com.asap.todoexmple.service.BaseRepository
import com.asap.todoexmple.service.generateTimestampWithRandom
import com.asap.todoexmple.util.SessionManager

class SmsRepository : BaseRepository() {
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
    suspend fun saveSmsData(context: Context, sender: String?, content: String?, messageId: Int): Boolean {
        try {
            val userId = SessionManager.Session.getUserId(context) ?: return false

            return executeDbOperation { connection ->
                val uniqueMessageId = if (isMessageIdExists(connection, messageId)) {
                    // 如果存在冲突，重新生成
                    generateUniqueMessageId(connection)
                } else {
                    messageId
                }
                Log.d("SmsRepository", "开始保存短信数据: sender=$sender, userId=$userId")
                val sql = """
                            INSERT INTO Messages (app_name, sender, content, user_id, message_id, date) 
                            VALUES ('SMS', ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """
                connection.prepareStatement(sql).use { stmt ->

                    stmt.setString(1, sender ?: "无发送者")
                    stmt.setString(2, content ?: "无内容")
                    stmt.setString(3, userId)
                    stmt.setInt(4, uniqueMessageId)
                    val result = stmt.executeUpdate() > 0
                    Log.d("SmsRepository", "短信数据保存结果: $result")
                    result
                }
            }
        } catch (e: Exception) {
            Log.e("SmsRepository", "保存短信数据失败", e)
            throw e
        }
    }
}