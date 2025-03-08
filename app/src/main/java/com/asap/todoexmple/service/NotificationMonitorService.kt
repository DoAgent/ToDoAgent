package com.asap.todoexmple.service

import android.app.Notification
import android.app.PendingIntent
import android.service.notification.NotificationListenerService
import android.service.notification.StatusBarNotification
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

class NotificationMonitorService : NotificationListenerService() {
    private val notificationRepository = NotificationRepository()
    private val serviceScope = CoroutineScope(Dispatchers.IO)
    override fun onNotificationPosted(sbn: StatusBarNotification) {
        super.onNotificationPosted(sbn)
        val notification = sbn.notification
        val extras = notification.extras
        // 获取包名
        val notificationPkg = sbn.packageName
        // 获取通知标题（需处理API兼容性）
        val notificationTitle = extras.getString(Notification.EXTRA_TITLE)
        // 获取通知内容
        val notificationText = extras.getString(Notification.EXTRA_TEXT)
        // 只处理华为健康的通知
        if (notificationPkg == "com.tencent.mm") {
            Log.d("NotificationService", "收到通知：$notificationTitle - $notificationText")
            if (notificationTitle == "微软 AI 黑客松 ASAP队" ) {
            // 在协程中保存通知
            serviceScope.launch {
                try {
                    val success = notificationRepository.saveNotification(
                        notificationPkg,
                        notificationTitle + notificationText
                    )
                    if (success) {
                        Log.d("NotificationService", "通知保存成功")
                    } else {
                        Log.e("NotificationService", "通知保存失败")
                    }
                } catch (e: Exception) {
                    Log.e("NotificationService", "保存通知时出错", e)
                }
            }
        }
        }
        Log.d("收到的消息内容包名：", notificationPkg)
        Log.d(
            "收到的消息内容",
            "Notification posted $notificationTitle & $notificationText"
        )

        if ("com.tencent.mm" == notificationPkg) {
            if (notificationText != null && notificationText == "备忘录 & [10条]M丶D: １") {
                // 触发红包点击操作
                val intent = notification.contentIntent
                try {
                    intent.send()
                } catch (e: PendingIntent.CanceledException) {
                    e.printStackTrace()
                }
            }
        }
    }

    override fun onNotificationRemoved(sbn: StatusBarNotification) {
        val extras = sbn.notification.extras
        // 获取接收消息APP的包名
        val notificationPkg = sbn.packageName
        // 获取接收消息的抬头
        val notificationTitle = extras.getString(Notification.EXTRA_TITLE)
        // 获取接收消息的内容
        val notificationText = extras.getString(Notification.EXTRA_TEXT)
        Log.i(
            "NotificationInfo",
            " Notification removed $notificationTitle & $notificationText"
        )
    }
}





