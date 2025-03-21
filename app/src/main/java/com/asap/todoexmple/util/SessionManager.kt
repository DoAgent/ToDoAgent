package com.asap.todoexmple.util

import android.content.Context
import android.content.SharedPreferences
import android.util.Log
import com.asap.todoexmple.util.SessionManager.Session.getUserId

object SessionManager {
    private const val TAG = "SessionManager"
    private const val PREF_NAME = "user_session"
    
    // SharedPreferences 键值常量
    private object PreferenceKeys {
        const val USER_ID = "user_id"
        const val USER_NAME = "user_name"
        const val USER_TOKEN = "user_token"
        const val LOGIN_TIME = "login_time"
        const val LAST_SYNC_TIME = "last_sync_time"
    }

    // 会话配置
    private object SessionConfig {
        const val SESSION_VALIDITY_PERIOD = 24 * 60 * 60 * 1000L // 24小时
        const val TOKEN_LENGTH = 32
    }

    // 用户登录相关方法
    object Auth {
        /**
         * 常规登录
         */
        fun login(context: Context, userId: String, username: String) {
            try {
                val token = Security.generateToken(userId)
                
                // 保存会话信息
                saveSessionData(context, userId, username, token)
                
                // 初始化用户数据
                initializeUserData(context, userId, username)
                
                Log.d(TAG, "用户登录成功：$userId")
            } catch (e: Exception) {
                Log.e(TAG, "登录失败: ${e.message}")
                throw LoginException("登录失败", e)
            }
        }

        /**
         * 注册后直接登录
         */
        fun loginAfterRegister(context: Context, userId: String, username: String) {
            try {
                val token = Security.generateToken(userId)
                
                // 保存会话信息
                saveSessionData(context, userId, username, token)
                
                // 初始化用户数据
                initializeUserData(context, userId, username)
                
                Log.d(TAG, "注册后直接登录成功：$userId")
            } catch (e: Exception) {
                Log.e(TAG, "注册后登录失败: ${e.message}")
                throw LoginException("注册后登录失败", e)
            }
        }

        /**
         * 登出
         */
        fun logout(context: Context) {
            try {
                val userId = getUserId(context)
                
                // 清理所有会话数据
                clearSessionData(context)
                
                // 清理用户数据
                cleanupUserData(context, userId)
                
                Log.d(TAG, "用户登出成功")
            } catch (e: Exception) {
                Log.e(TAG, "登出过程中出错: ${e.message}")
                throw LogoutException("登出失败", e)
            }
        }

        private fun saveSessionData(context: Context, userId: String, username: String, token: String) {
            getPrefs(context).edit().apply {
                putString(PreferenceKeys.USER_ID, userId)
                putString(PreferenceKeys.USER_NAME, username)
                putString(PreferenceKeys.USER_TOKEN, token)
                putLong(PreferenceKeys.LOGIN_TIME, System.currentTimeMillis())
                apply()
            }
        }

        private fun clearSessionData(context: Context) {
            getPrefs(context).edit().clear().apply()
        }

        private fun initializeUserData(context: Context, userId: String, username: String) {
            // 保存用户信息到本地数据库
            LocalDatabaseHelper(context).use { helper ->
                helper.saveUserInfo(userId, username)
            }
            
            // 设置数据同步
            LocalDatabaseHelper.setupPeriodicSync(context, userId)
        }

        private fun cleanupUserData(context: Context, userId: String?) {
            // 只取消同步任务，不删除数据
            userId?.let { 
                try {
                    LocalDatabaseHelper.cancelSync(context, it)
                    Log.d(TAG, "已取消用户 $it 的同步任务")
                } catch (e: Exception) {
                    Log.e(TAG, "取消同步任务失败: ${e.message}")
                }
            }
        }
    }

    // 会话状态相关方法
    object Session {
        fun isLoggedIn(context: Context): Boolean {
            val prefs = getPrefs(context)
            val userId = prefs.getString(PreferenceKeys.USER_ID, null)
            val loginTime = prefs.getLong(PreferenceKeys.LOGIN_TIME, 0)
            
            return !userId.isNullOrEmpty() && Security.isSessionValid(loginTime)
        }

        fun getUserId(context: Context): String? {
            return getPrefs(context).getString(PreferenceKeys.USER_ID, null)
        }

        fun getUsername(context: Context): String? {
            return getPrefs(context).getString(PreferenceKeys.USER_NAME, null)
        }

        fun getUserInfo(context: Context): Pair<String, String>? {
            val prefs = getPrefs(context)
            val userId = prefs.getString(PreferenceKeys.USER_ID, null)
            val username = prefs.getString(PreferenceKeys.USER_NAME, null)
            
            return if (userId != null && username != null) {
                Pair(userId, username)
            } else {
                null
            }
        }
    }

    // 同步相关方法
    object Sync {
        fun updateLastSyncTime(context: Context) {
            getPrefs(context).edit()
                .putLong(PreferenceKeys.LAST_SYNC_TIME, System.currentTimeMillis())
                .apply()
        }

        fun getLastSyncTime(context: Context): Long {
            return getPrefs(context).getLong(PreferenceKeys.LAST_SYNC_TIME, 0)
        }
    }

    // 安全相关方法
    private object Security {
        fun generateToken(userId: String): String {
            val timestamp = System.currentTimeMillis()
            val random = (1000..9999).random()
            val randomString = (1..8).map { ('A'..'Z').random() }.joinToString("")
            return "$userId${timestamp}$random$randomString"
        }

        fun isSessionValid(loginTime: Long): Boolean {
            return System.currentTimeMillis() - loginTime < SessionConfig.SESSION_VALIDITY_PERIOD
        }
    }

    // 工具方法
    private fun getPrefs(context: Context): SharedPreferences {
        return context.getSharedPreferences(PREF_NAME, Context.MODE_PRIVATE)
    }

    // 自定义异常类
    class LoginException(message: String, cause: Throwable? = null) : Exception(message, cause)
    class LogoutException(message: String, cause: Throwable? = null) : Exception(message, cause)
} 