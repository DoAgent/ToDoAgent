package com.asap.todoexmple.receiver

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.util.Log
import com.asap.todoexmple.util.LocalDatabaseHelper

class BatteryReceiver : BroadcastReceiver() {
    companion object {
        private const val TAG = "BatteryReceiver"
    }

    override fun onReceive(context: Context?, intent: Intent?) {
        if (context == null || intent == null) return

        try {
            when (intent.action) {
                Intent.ACTION_BATTERY_LOW -> {
                    // 电量低时触发立即同步
                    val userId = getCurrentUserId(context)
                    if (userId != null) {
                        LocalDatabaseHelper.startImmediateSync(context, userId)
                        Log.d(TAG, "电量低，触发立即同步")
                    }
                }
                Intent.ACTION_POWER_CONNECTED -> {
                    // 连接充电器时触发立即同步
                    val userId = getCurrentUserId(context)
                    if (userId != null) {
                        LocalDatabaseHelper.startImmediateSync(context, userId)
                        Log.d(TAG, "充电器已连接，触发立即同步")
                    }
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "处理电池广播事件失败", e)
        }
    }

    private fun getCurrentUserId(context: Context): String? {
        // 从 SharedPreferences 或其他存储中获取当前用户ID
        // 这里需要根据你的应用实际情况来实现
        return context.getSharedPreferences("user_prefs", Context.MODE_PRIVATE)
            .getString("current_user_id", null)
    }
}
