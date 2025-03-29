package com.asap.todoexmple.util

import android.content.Context
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.os.Build
import android.os.PowerManager
import android.util.Log
import androidx.work.*
import com.asap.todoexmple.util.DatabaseHelper.Companion.getConnection
import com.asap.todoexmple.util.DatabaseHelper.Companion.releaseConnection
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue
import kotlinx.coroutines.*
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import java.util.concurrent.atomic.AtomicBoolean

class DatabaseHelper {
    companion object {
        private const val TAG = "DatabaseHelper"
        private const val HOST = "103.116.245.150"
        private const val PORT = "3306"
        private const val DATABASE = "ToDoAgent"
        private const val USERNAME = "root"
        private const val PASSWORD = "4bc6bc963e6d8443453676"

        private const val POOL_SIZE = 3
        private val connectionPool = ConcurrentLinkedQueue<Connection>()
        private val connectionGuard = Any() // 添加连接保护锁
        private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        private val isInitializing = AtomicBoolean(false)
        private var applicationContext: Context? = null

        init {
            initializePool()
        }

        fun initialize(context: Context) {
            applicationContext = context.applicationContext
            initializePool()
        }

        private fun initializePool() {
            if (isInitializing.getAndSet(true)) return

            scope.launch {
                var retryCount = 0
                val maxRetries = 5
                
                while (retryCount < maxRetries && connectionPool.size < POOL_SIZE) {
                    if (!isNetworkAvailable()) {
                        delay(3000) // 等待3秒
                        continue
                    }

                    try {
                        createNewConnection()?.let { connection ->
                            if (testConnection(connection)) {
                                connectionPool.offer(connection)
                            } else {
                                try { connection.close() } catch (_: Exception) {}
                            }
                        }
                    } catch (e: Exception) {
                        retryCount++
                        delay(2000L * (retryCount + 1)) // 指数退避
                    }
                }

                isInitializing.set(false)
            }
        }

        private fun createNewConnection(): Connection? {
            Log.d(TAG, "正在创建新的数据库连接")
            return try {
                Class.forName("com.mysql.jdbc.Driver")
                val url = "jdbc:mysql://$HOST:$PORT/$DATABASE?" +
                        "useSSL=false&" +
                        "useUnicode=true&" +
                        "characterEncoding=UTF-8&" +
                        "connectionCollation=utf8mb4_unicode_ci&" +
                        "characterSetResults=UTF-8&" +
                        "autoReconnect=true&" +
                        "connectTimeout=30000&" +
                        "socketTimeout=30000&" +
                        "serverTimezone=Asia/Shanghai"

                val props = Properties().apply {
                    setProperty("user", USERNAME)
                    setProperty("password", PASSWORD)
                    setProperty("connectTimeout", "30000")
                    setProperty("socketTimeout", "30000")
                }

                val connection = DriverManager.getConnection(url, props)
                Log.d(TAG, "数据库连接创建成功")
                connection
            } catch (e: Exception) {
                Log.e(TAG, "创建数据库连接失败", e)
                null
            }
        }

        private fun testConnection(connection: Connection): Boolean {
            Log.d(TAG, "正在测试连接有效性")
            return try {
                val valid = connection.isValid(5) && connection.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT 1").use { rs ->
                        rs.next()
                    }
                }
                Log.d(TAG, "连接测试结果: $valid")
                valid
            } catch (e: Exception) {
                Log.e(TAG, "测试连接时发生错误", e)
                false
            }
        }

        private fun isNetworkAvailable(): Boolean {
            val context = applicationContext ?: return false
            val connectivityManager = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
                ?: return false

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                val network = connectivityManager.activeNetwork ?: return false
                val capabilities = connectivityManager.getNetworkCapabilities(network) ?: return false
                return capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
            } else {
                @Suppress("DEPRECATION")
                return connectivityManager.activeNetworkInfo?.isConnected == true
            }
        }

        private fun startConnectionMonitor() {
            thread(start = true, isDaemon = true) {
                while (true) {
                    try {
                        synchronized(connectionGuard) {
                            val iterator = connectionPool.iterator()
                            while (iterator.hasNext()) {
                                val connection = iterator.next()
                                if (connection.isClosed || !connection.isValid(5)) {
                                    iterator.remove()
                                    createNewConnection()?.let { connectionPool.offer(it) }
                                }
                            }
                            
                            // 确保连接池保持最小连接数
                            while (connectionPool.size < POOL_SIZE) {
                                createNewConnection()?.let { connectionPool.offer(it) }
                            }
                        }
                        Thread.sleep(30000) // 每30秒检查一次
                    } catch (e: Exception) {
                        // 连接监控异常处理
                    }
                }
            }
        }

        suspend fun checkAndRepairConnections() {
            withContext(Dispatchers.IO) {
                val connectionsToAdd = mutableListOf<Connection>()
                
                synchronized(connectionGuard) {
                    try {
                        val iterator = connectionPool.iterator()
                        while (iterator.hasNext()) {
                            val connection = iterator.next()
                            try {
                                if (connection.isClosed || !connection.isValid(5)) {
                                    iterator.remove()
                                    connection.close()
                                }
                            } catch (e: Exception) {
                                iterator.remove()
                                try { connection.close() } catch (_: Exception) {}
                            }
                        }

                        val neededConnections = POOL_SIZE - connectionPool.size
                        repeat(neededConnections) {
                            createNewConnection()?.let { connectionsToAdd.add(it) }
                        }
                    } catch (e: Exception) {
                        // 检查连接失败处理
                    }
                }
                
                // 在同步块外添加新连接
                connectionsToAdd.forEach { connection ->
                    synchronized(connectionGuard) {
                        if (connectionPool.size < POOL_SIZE) {
                            connectionPool.offer(connection)
                        } else {
                            try { connection.close() } catch (_: Exception) {}
                        }
                    }
                }
            }
        }

        suspend fun getConnection(): Connection? = withContext(Dispatchers.IO) {
            Log.d(TAG, "正在获取数据库连接")
            for (attempt in 1..3) {
                Log.d(TAG, "尝试获取连接: 第$attempt 次")
                
                // 如果连接池为空，尝试初始化
                if (connectionPool.isEmpty() && !isInitializing.get()) {
                    Log.d(TAG, "连接池为空，正在初始化")
                    initializePool()
                }

                synchronized(connectionGuard) {
                    connectionPool.poll()?.let { connection ->
                        if (testConnection(connection)) {
                            Log.d(TAG, "成功获取有效连接")
                            return@withContext connection
                        }
                        Log.d(TAG, "连接无效，尝试关闭")
                        try { connection.close() } catch (e: Exception) {
                            Log.e(TAG, "关闭无效连接失败", e)
                        }
                    }
                }

                // 如果没有可用连接，创建新连接
                createNewConnection()?.let { connection ->
                    if (testConnection(connection)) {
                        Log.d(TAG, "成功创建新连接")
                        return@withContext connection
                    }
                    Log.d(TAG, "新创建的连接无效，尝试关闭")
                    try { connection.close() } catch (e: Exception) {
                        Log.e(TAG, "关闭无效连接失败", e)
                    }
                }

                Log.d(TAG, "获取连接失败，等待重试")
                delay(2000L * attempt)
            }
            Log.e(TAG, "所有获取连接尝试都失败")
            null
        }

        suspend fun releaseConnection(connection: Connection?) {
            if (connection == null) return
            
            withContext(Dispatchers.IO) {
                synchronized(connectionGuard) {
                    try {
                        if (!connection.isClosed && connection.isValid(5)) {
                            if (connectionPool.size < POOL_SIZE) {
                                connectionPool.offer(connection)
                                return@withContext
                            }
                        }
                        connection.close()
                    } catch (e: Exception) {
                        try { connection.close() } catch (_: Exception) {}
                    }
                }
            }
        }

        fun closeAllConnections() {
            synchronized(connectionGuard) {
                connectionPool.forEach { conn ->
                    try { conn.close() } catch (_: Exception) {}
                }
                connectionPool.clear()
                scope.cancel()
            }
        }

        fun setupBackgroundWork(context: Context) {
            val workRequest = PeriodicWorkRequestBuilder<DatabaseWorker>(
                15, TimeUnit.MINUTES,
                5, TimeUnit.MINUTES
            ).setConstraints(
                Constraints.Builder()
                    .setRequiredNetworkType(NetworkType.CONNECTED)
                    .build()
            ).build()

            WorkManager.getInstance(context)
                .enqueueUniquePeriodicWork(
                    "database_sync",
                    ExistingPeriodicWorkPolicy.KEEP,
                    workRequest
                )
        }

        suspend fun checkUsernameAvailable(username: String): Boolean = withContext(Dispatchers.IO) {
            Log.d(TAG, "正在检查用户名可用性: $username")
            
            val connection = getConnection()
            if (connection == null) {
                Log.e(TAG, "数据库连接失败")
                throw Exception("无法连接到数据库服务器")
            }

            try {


                // 检查用户名是否存在
                withContext(Dispatchers.IO) {
                    connection.prepareStatement(
                        "SELECT COUNT(*) FROM Users WHERE user_name = ?"
                    ).use { stmt ->
                        stmt.setString(1, username)
                        stmt.executeQuery().use { rs ->
                            if (rs.next()) {
                                val count = rs.getInt(1)
                                Log.d(TAG, "用户名检查结果: count=$count")
                                return@withContext count == 0
                            }
                            Log.e(TAG, "查询结果为空")
                            throw Exception("查询结果异常")
                        }
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "检查用户名时发生错误", e)
                throw Exception("数据库查询失败: ${e.message}")
            } finally {
                withContext(Dispatchers.IO) {
                    try {
                        releaseConnection(connection)
                    } catch (e: Exception) {
                        Log.e(TAG, "释放连接时发生错误", e)
                    }
                }
            }
        }

        suspend fun checkEmailAvailable(email: String): Boolean = withContext(Dispatchers.IO) {
            Log.d(TAG, "正在检查邮箱可用性: $email")
            
            val connection = getConnection()
            if (connection == null) {
                Log.e(TAG, "数据库连接失败")
                throw Exception("无法连接到数据库服务器")
            }

            try {


                // 检查邮箱是否存在
                withContext(Dispatchers.IO) {
                    connection.prepareStatement(
                        "SELECT COUNT(*) FROM Users WHERE user_mail = ?"
                    ).use { stmt ->
                        stmt.setString(1, email)
                        stmt.executeQuery().use { rs ->
                            if (rs.next()) {
                                val count = rs.getInt(1)
                                Log.d(TAG, "邮箱检查结果: count=$count")
                                return@withContext count == 0
                            }
                            Log.e(TAG, "查询结果为空")
                            throw Exception("查询结果异常")
                        }
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "检查邮箱时发生错误", e)
                throw Exception("数据库查询失败: ${e.message}")
            } finally {
                withContext(Dispatchers.IO) {
                    try {
                        releaseConnection(connection)
                    } catch (e: Exception) {
                        Log.e(TAG, "释放连接时发生错误", e)
                    }
                }
            }
        }

        suspend fun generateUniqueUserId(): String {
            val connection = getConnection() ?: throw Exception("无法连接到数据库")
            try {
                while (true) {
                    val userId = generateRandomUserId()
                    val stmt = connection.prepareStatement("SELECT COUNT(*) FROM Users WHERE user_id = ?")
                    stmt.setString(1, userId)
                    val rs = stmt.executeQuery()
                    rs.next()
                    if (rs.getInt(1) == 0) {
                        return userId
                    }
                }
            } finally {
                releaseConnection(connection)
            }
        }

        private fun generateRandomUserId(): String {
            return (1..8).map { ('0'..'9').random() }.joinToString("")
        }

        suspend fun registerUser(
            username: String, 
            email: String, 
            hashedPassword: String,
            salt: String
        ): String? = withContext(Dispatchers.IO) {
            try {
                val userId = generateUniqueUserId()
                val connection = getConnection() ?: throw Exception("无法连接到数据库")

                try {
                    connection.prepareStatement("""
                        INSERT INTO Users (user_id, user_name, user_mail,user_phone, pwd_hash, pwd_salt) 
                        VALUES (?, ?, ?,'1', ?, ?)
                    """).use { stmt ->
                        stmt.setString(1, userId)
                        stmt.setString(2, username)
                        stmt.setString(3, email)
                        stmt.setString(4, hashedPassword)
                        stmt.setString(5, salt)
                        stmt.executeUpdate()
                    }
                    userId
                } finally {
                    releaseConnection(connection)
                }
            } catch (e: Exception) {
                Log.e(TAG, "注册用户失败", e)
                null
            }
        }
    }
}

class DatabaseWorker(
    context: Context,
    params: WorkerParameters
) : CoroutineWorker(context, params) {
    
    override suspend fun doWork(): Result {
        val powerManager = applicationContext.getSystemService(Context.POWER_SERVICE) as PowerManager
        val wakeLock = powerManager.newWakeLock(
            PowerManager.PARTIAL_WAKE_LOCK,
            "ASAP:DatabaseWorkerWakeLock"
        )
        
        wakeLock.acquire(10 * 60 * 1000L) // 10分钟超时
        
        try {
            DatabaseHelper.checkAndRepairConnections()
            return Result.success()
            } catch (e: Exception) {
            return Result.retry()
        } finally {
            if (wakeLock.isHeld) {
                wakeLock.release()
            }
        }
    }
}
