package com.asap.todoexmple.service

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import com.asap.todoexmple.util.DatabaseHelper
import java.sql.Connection

abstract class BaseRepository {
    protected suspend fun executeDbOperation(operation: suspend (Connection) -> Boolean): Boolean {
        return withContext(Dispatchers.IO) {
            var connection: Connection? = null
            var retryCount = 0
            val maxRetries = 3

            while (retryCount < maxRetries) {
                try {
                    connection = DatabaseHelper.getConnection()
                    if (connection != null) {
                        val result = operation(connection)
                        if (result) return@withContext true
                    }
                } catch (e: Exception) {
                    Log.e("BaseRepository", "数据库操作失败 (尝试 ${retryCount + 1}): ${e.message}")
                    delay(1000L * (retryCount + 1))
                } finally {
                    DatabaseHelper.releaseConnection(connection)
                }
                retryCount++
            }
            false
        }
    }
}
