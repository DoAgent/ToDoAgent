package com.asap.todoexmple.service

import android.app.Notification
import android.app.PendingIntent
import android.service.notification.NotificationListenerService
import android.service.notification.StatusBarNotification
import android.util.Log

class NotificationMonitorService : NotificationListenerService() {
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





