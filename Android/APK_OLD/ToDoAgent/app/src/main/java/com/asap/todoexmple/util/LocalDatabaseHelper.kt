package com.asap.todoexmple.util

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import android.Manifest
import android.provider.CalendarContract
import android.content.ContentValues
import android.content.pm.PackageManager
import android.util.Log
import androidx.core.content.ContextCompat
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.*
import java.text.SimpleDateFormat

class LocalDatabaseHelper(context: Context) : SQLiteOpenHelper(context, DATABASE_NAME, null, DATABASE_VERSION) {
    companion object {
        private const val TAG = "LocalDatabaseHelper"
        private const val DATABASE_NAME = "user_settings.db"
        private const val DATABASE_VERSION = 4  // 升级版本以支持用户名

        // SQL 语句常量
        private const val CREATE_USER_SETTINGS_TABLE = """
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id TEXT PRIMARY KEY,
                user_name TEXT NOT NULL,
                keep_alive_boot INTEGER DEFAULT 0,
                keep_alive_battery INTEGER DEFAULT 0,
                keep_alive_hidden INTEGER DEFAULT 0,
                dark_mode INTEGER DEFAULT 0,
                language TEXT DEFAULT 'zh'
            )
        """

        private const val CREATE_TODO_LIST_TABLE = """
            CREATE TABLE IF NOT EXISTS ToDoListLocal (
                list_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                start_time TEXT,
                end_time TEXT,
                location TEXT,
                todo_content TEXT,
                is_important INTEGER DEFAULT 0,
                is_completed INTEGER DEFAULT 0,
                sync_status INTEGER DEFAULT 0,
                last_modified TEXT,
                FOREIGN KEY (user_id) REFERENCES user_settings(user_id) ON DELETE CASCADE
            )
        """

        // 添加日历权限常量
        private const val CALENDAR_PERMISSION = Manifest.permission.WRITE_CALENDAR

        // 静态工具方法
        fun saveUserInfo(context: Context, userId: String, username: String) {
            LocalDatabaseHelper(context).use { helper ->
                helper.saveUserInfo(userId, username)
            }
        }

        fun clearAllUserData(context: Context) {
            LocalDatabaseHelper(context).use { helper ->
                helper.clearUserData()
            }
        }

        fun setupPeriodicSync(context: Context, userId: String) {
            try {
                val syncData = androidx.work.Data.Builder()
                    .putString("user_id", userId)
                    .build()

                val syncRequest = androidx.work.PeriodicWorkRequestBuilder<ToDoSyncWorker>(
                    1, java.util.concurrent.TimeUnit.MINUTES,
                    30, java.util.concurrent.TimeUnit.SECONDS
                )
                    .setInputData(syncData)
                    .setConstraints(
                        androidx.work.Constraints.Builder()
                            .setRequiredNetworkType(androidx.work.NetworkType.CONNECTED)
                            .build()
                    )
                    .addTag("todo_sync")
                    .build()

                androidx.work.WorkManager.getInstance(context)
                    .enqueueUniquePeriodicWork(
                        "todo_sync_$userId",
                        androidx.work.ExistingPeriodicWorkPolicy.REPLACE,
                        syncRequest
                    )
                
                Log.d(TAG, "已设置定期同步任务")
            } catch (e: Exception) {
                Log.e(TAG, "设置定期同步失败", e)
            }
        }

        fun cancelSync(context: Context, userId: String) {
            try {
                androidx.work.WorkManager.getInstance(context).apply {
                    cancelUniqueWork("todo_sync_$userId")
                    cancelUniqueWork("todo_immediate_sync_$userId")
                }
                Log.d(TAG, "已取消同步任务")
            } catch (e: Exception) {
                Log.e(TAG, "取消同步任务失败", e)
            }
        }

        // 添加立即同步方法
        fun startImmediateSync(context: Context, userId: String) {
            try {
                val syncData = androidx.work.Data.Builder()
                    .putString("user_id", userId)
                    .build()

                val syncRequest = androidx.work.OneTimeWorkRequestBuilder<ToDoSyncWorker>()
                    .setInputData(syncData)
                    .setConstraints(
                        androidx.work.Constraints.Builder()
                            .setRequiredNetworkType(androidx.work.NetworkType.CONNECTED)
                            .build()
                    )
                    .addTag("todo_sync_immediate")
                    .build()

                androidx.work.WorkManager.getInstance(context)
                    .enqueueUniqueWork(
                        "todo_immediate_sync_$userId",
                        androidx.work.ExistingWorkPolicy.REPLACE,
                        syncRequest
                    )
                
                Log.d(TAG, "已触发立即同步")
            } catch (e: Exception) {
                Log.e(TAG, "触发立即同步失败", e)
            }
        }

        // 添加初始化方法
        fun initializeDatabaseIfNeeded(context: Context) {
            try {
                LocalDatabaseHelper(context).initializeDatabase()
                Log.d(TAG, "数据库初始化检查完成")
            } catch (e: Exception) {
                Log.e(TAG, "数据库初始化失败", e)
            }
        }
    }

    // 数据库创建和升级
    override fun onCreate(db: SQLiteDatabase) {
        try {
            db.beginTransaction()
            try {
                // 确保先创建用户设置表
                db.execSQL(CREATE_USER_SETTINGS_TABLE)
                Log.d(TAG, "用户设置表创建成功")
                
                // 然后创建待办事项表
                db.execSQL(CREATE_TODO_LIST_TABLE)
                Log.d(TAG, "待办事项表创建成功")
                
                // 创建索引
                db.execSQL("CREATE INDEX IF NOT EXISTS idx_todo_user_id ON ToDoListLocal(user_id)")
                
                // 插入默认数据（如果需要的话）
                insertDefaultSettings(db)
                
                db.setTransactionSuccessful()
                Log.d(TAG, "数据库创建成功")
            } finally {
                db.endTransaction()
            }
        } catch (e: Exception) {
            Log.e(TAG, "数据库创建失败", e)
            throw e  // 抛出异常以便知道创建失败
        }
    }

    private fun insertDefaultSettings(db: SQLiteDatabase) {
        // 插入一个默认用户设置记录
        val values = android.content.ContentValues().apply {
            put("user_id", "default_user")
            put("user_name", "默认用户")
            put("keep_alive_boot", 0)
            put("keep_alive_battery", 0)
            put("keep_alive_hidden", 0)
            put("dark_mode", 0)
            put("language", "zh")
        }
        
        db.insertWithOnConflict(
            "user_settings",
            null,
            values,
            SQLiteDatabase.CONFLICT_IGNORE
        )
    }

    // 添加一个初始化检查方法
    fun initializeDatabase() {
        try {
            writableDatabase.use { db ->
                // 检查表是否存在
                val cursor = db.rawQuery(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='ToDoListLocal'",
                    null
                )
                
                cursor.use { 
                    if (!cursor.moveToFirst()) {
                        // 如果表不存在，重新创建
                        onCreate(db)
                    }
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "数据库初始化检查失败", e)
            throw e
        }
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        try {
            db.beginTransaction()
            try {
                if (oldVersion < 4) {
                    // 备份旧数据
                    db.execSQL("CREATE TABLE IF NOT EXISTS user_settings_backup AS SELECT * FROM user_settings")
                    // 删除旧表
                    db.execSQL("DROP TABLE IF EXISTS user_settings")
                    // 创建新表
                    db.execSQL(CREATE_USER_SETTINGS_TABLE)
                    // 恢复数据
                    db.execSQL("""
                        INSERT INTO user_settings (
                            user_id, keep_alive_boot, keep_alive_battery, 
                            keep_alive_hidden, dark_mode, language
                        )
                        SELECT 
                            user_id, keep_alive_boot, keep_alive_battery, 
                            keep_alive_hidden, dark_mode, language
                        FROM user_settings_backup
                    """)
                    // 删除备份表
                    db.execSQL("DROP TABLE IF EXISTS user_settings_backup")
                }
                
                db.setTransactionSuccessful()
                Log.d(TAG, "数据库升级成功")
            } finally {
                db.endTransaction()
            }
        } catch (e: Exception) {
            Log.e(TAG, "数据库升级失败", e)
        }
    }

    // 用户信息管理方法
    fun saveUserInfo(userId: String, username: String) {
        writableDatabase.use { db ->
            try {
                val values = android.content.ContentValues().apply {
                    put("user_id", userId)
                    put("user_name", username)
                }
                
                db.insertWithOnConflict(
                    "user_settings",
                    null,
                    values,
                    SQLiteDatabase.CONFLICT_REPLACE
                )
                
                Log.d(TAG, "用户信息保存成功")
            } catch (e: Exception) {
                Log.e(TAG, "保存用户信息失败", e)
                throw e
            }
        }
    }

    fun clearUserData() {
        writableDatabase.use { db ->
            try {
                db.beginTransaction()
                try {
                    db.delete("user_settings", null, null)
                    db.delete("ToDoListLocal", null, null)
                    db.setTransactionSuccessful()
                    Log.d(TAG, "用户数据清除成功")
                } finally {
                    db.endTransaction()
                }
            } catch (e: Exception) {
                Log.e(TAG, "清除用户数据失败", e)
                throw e
            }
        }
    }

    // 同步相关方法
    suspend fun syncToDoListFromCloud(userId: String) = withContext(Dispatchers.IO) {
        if (!isValidUser(userId)) {
            Log.e(TAG, "同步失败：无效用户ID")
            return@withContext
        }

        val dbHelper = DatabaseHelper.Companion
        dbHelper.getConnection()?.use { connection ->
            try {
                val lastSyncTime = getLastSyncTime(userId)
                
                connection.prepareStatement("""
                    SELECT list_id, user_id, start_time, end_time, location, todo_content 
                    FROM ToDoList 
                    WHERE user_id = ? AND last_modified > ?
                """).use { stmt ->
                    stmt.setString(1, userId)
                    stmt.setString(2, lastSyncTime)
                    
                    val rs = stmt.executeQuery()
                    writableDatabase.use { db ->
                        db.beginTransaction()
                        try {
                            while (rs.next()) {
                                updateLocalTodoItem(db, rs)
                            }
                            updateLastSyncTime(db, userId)
                            db.setTransactionSuccessful()
                        } finally {
                            db.endTransaction()
                        }
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "同步失败", e)
            }
        }
    }

    // 辅助方法
    fun isValidUser(userId: String): Boolean {
        return readableDatabase.use { db ->
            db.query(
                "user_settings",
                arrayOf("user_id"),
                "user_id = ?",
                arrayOf(userId),
                null, null, null
            ).use { cursor ->
                cursor.moveToFirst()
            }
        }
    }

    private fun updateLocalTodoItem(db: SQLiteDatabase, rs: java.sql.ResultSet) {
        val values = android.content.ContentValues().apply {
            put("list_id", rs.getString("list_id"))
            put("user_id", rs.getString("user_id"))
            put("start_time", rs.getString("start_time"))
            put("end_time", rs.getString("end_time"))
            put("location", rs.getString("location"))
            put("todo_content", rs.getString("todo_content"))
            put("sync_status", 1)
            put("last_modified", System.currentTimeMillis().toString())
        }
        
        db.insertWithOnConflict(
            "ToDoListLocal",
            null,
            values,
            SQLiteDatabase.CONFLICT_REPLACE
        )
    }

    private fun getLastSyncTime(userId: String): String = "1970-01-01 00:00:00"

    private fun updateLastSyncTime(db: SQLiteDatabase, userId: String) {
        // 更新最后同步时间的实现
    }
    // 在需要创建日历提醒的地方
    //localDatabaseHelper.createCalendarReminder(context, todoListId)
    // 添加创建日历提醒的方法
    fun createCalendarReminder(context: Context, listId: String) {
        if (ContextCompat.checkSelfPermission(context, CALENDAR_PERMISSION)
            != PackageManager.PERMISSION_GRANTED) {
            Log.e(TAG, "没有日历权限")
            return
        }

        readableDatabase.use { db ->
            val cursor = db.query(
                "ToDoListLocal",
                arrayOf("start_time", "end_time", "location", "todo_content"),
                "list_id = ?",
                arrayOf(listId),
                null, null, null
            )

            cursor.use { 
                if (cursor.moveToFirst()) {
                    val startTime = cursor.getString(cursor.getColumnIndexOrThrow("start_time"))
                    val endTime = cursor.getString(cursor.getColumnIndexOrThrow("end_time"))
                    val location = cursor.getString(cursor.getColumnIndexOrThrow("location"))
                    val content = cursor.getString(cursor.getColumnIndexOrThrow("todo_content"))

                    try {
                        // 获取默认日历ID
                        val calendarId = getDefaultCalendarId(context)
                        
                        // 创建事件
                        val values = ContentValues().apply {
                            put(CalendarContract.Events.CALENDAR_ID, calendarId)
                            put(CalendarContract.Events.TITLE, content)
                            put(CalendarContract.Events.DESCRIPTION, content)
                            put(CalendarContract.Events.EVENT_LOCATION, location)
                            put(CalendarContract.Events.DTSTART, parseDateTime(startTime))
                            put(CalendarContract.Events.DTEND, parseDateTime(endTime))
                            put(CalendarContract.Events.EVENT_TIMEZONE, TimeZone.getDefault().id)
                        }

                        // 插入事件
                        val eventUri = context.contentResolver.insert(
                            CalendarContract.Events.CONTENT_URI, values)
                        
                        // 添加提醒（默认提前15分钟）
                        eventUri?.lastPathSegment?.let { eventId ->
                            val reminderValues = ContentValues().apply {
                                put(CalendarContract.Reminders.EVENT_ID, eventId)
                                put(CalendarContract.Reminders.MINUTES, 15)
                                put(CalendarContract.Reminders.METHOD, 
                                    CalendarContract.Reminders.METHOD_ALERT)
                            }
                            context.contentResolver.insert(
                                CalendarContract.Reminders.CONTENT_URI, reminderValues)
                        }

                        Log.d(TAG, "日历提醒创建成功")
                    } catch (e: Exception) {
                        Log.e(TAG, "创建日历提醒失败", e)
                    }
                }
            }
        }
    }

    // 获取默认日历ID
    private fun getDefaultCalendarId(context: Context): Long {
        val projection = arrayOf(CalendarContract.Calendars._ID)
        val selection = "${CalendarContract.Calendars.IS_PRIMARY} = 1"
        
        context.contentResolver.query(
            CalendarContract.Calendars.CONTENT_URI,
            projection,
            selection,
            null,
            null
        )?.use { cursor ->
            if (cursor.moveToFirst()) {
                return cursor.getLong(0)
            }
        }
        throw Exception("未找到默认日历")
    }

    // 解析日期时间字符串为时间戳
    private fun parseDateTime(dateTimeStr: String): Long {
        return SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault())
            .parse(dateTimeStr)?.time ?: System.currentTimeMillis()
    }
}

// 同步工作器
class ToDoSyncWorker(
    context: Context,
    params: androidx.work.WorkerParameters
) : androidx.work.CoroutineWorker(context, params) {
    
    override suspend fun doWork(): Result {
        try {
            val userId = inputData.getString("user_id") ?: return Result.failure()
            
            LocalDatabaseHelper(applicationContext).use { helper ->
                if (!helper.isValidUser(userId)) {
                    Log.e("ToDoSyncWorker", "无效用户ID")
                    return Result.failure()
                }
                
                helper.syncToDoListFromCloud(userId)
            }
            
            return Result.success()
        } catch (e: Exception) {
            Log.e("ToDoSyncWorker", "同步失败", e)
            return Result.retry()
        }
    }
}