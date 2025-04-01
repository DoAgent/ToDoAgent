package com.asap.todoexmple.receiver

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import com.asap.todoexmple.service.NotificationMonitorService
import com.asap.todoexmple.service.SmsHandler

class ServiceRestartReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        when (intent.action) {
            Intent.ACTION_BOOT_COMPLETED,
            Intent.ACTION_MY_PACKAGE_REPLACED,
            "android.intent.action.USER_PRESENT" -> {
                // 启动通知监听服务
                KeepAliveUtils.startForegroundService(
                    context,
                    NotificationMonitorService::class.java
                )
                
                // 初始化短信处理服务，传入 applicationContext
                SmsHandler.getInstance(context.applicationContext)
            }
        }
    }
} 