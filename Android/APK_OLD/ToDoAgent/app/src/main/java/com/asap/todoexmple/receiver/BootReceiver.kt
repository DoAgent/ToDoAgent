package com.asap.todoexmple.receiver

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

import android.util.Log
import com.asap.todoexmple.activity.MainActivity


class BootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        when (intent.action) {
            Intent.ACTION_BOOT_COMPLETED,
            "android.intent.action.QUICKBOOT_POWERON",
            "android.intent.action.REBOOT" -> {
                // 检查是否启用了开机自启动
                if (KeepAliveUtils.isBootStartEnabled(context)) {
                    Log.d("BootReceiver", "系统启动完成，准备启动应用")
                    try {
                        // 启动主应用
                        val launchIntent = Intent(context, MainActivity::class.java).apply {
                            addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                            addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP)
                        }
                        context.startActivity(launchIntent)
                        Log.d("BootReceiver", "应用启动成功")
                    } catch (e: Exception) {
                        Log.e("BootReceiver", "应用启动失败: ${e.message}")
                    }
                }
            }
        }
    }
}