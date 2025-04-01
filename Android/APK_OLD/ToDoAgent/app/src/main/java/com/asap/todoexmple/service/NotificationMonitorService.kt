package com.asap.todoexmple.service

import android.annotation.SuppressLint
import android.app.Notification
import android.app.PendingIntent
import android.content.ComponentName
import android.content.Context
import android.service.notification.NotificationListenerService
import android.service.notification.StatusBarNotification
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


// 添加计数器对象
object MessageIdCounter {
    private var counter = 0

    @SuppressLint("DefaultLocale")
    @Synchronized
    fun getNext(): String {
        val current = counter
        counter = (counter + 1) % 100  // 到99后重置为0
        return String.format("%02d", current)  // 保持两位数格式
    }
}
fun generateTimestampWithRandom(): Int {
    // 获取当前时间并格式化
    val current = LocalDateTime.now()
    val timeFormatter = DateTimeFormatter.ofPattern("MMddHHmm")
    val timePart = current.format(timeFormatter)

    // 使用递增的计数器替代随机数
    val sequenceCode = MessageIdCounter.getNext()

    // 组合时间和序列号
    var messageId = (timePart + sequenceCode).toInt()
    Log.d("MessageID", "生成ID: $messageId (时间: $timePart, 序列: $sequenceCode)")
    return messageId
    //TODO:咱这个目前有个bug 如果一分钟内挤入100条massage会出问题，我暂时不想改了，先记录下来

}


class NotificationMonitorService : NotificationListenerService() {
    private val notificationRepository = NotificationRepository()
    private val serviceScope = CoroutineScope(Dispatchers.IO)

    private fun isAppEnabled(packageName: String): Boolean {
        val sharedPrefs = getSharedPreferences("notification_settings", Context.MODE_PRIVATE)
        return sharedPrefs.getBoolean(packageName, false)
    }

    private fun handleNotification(notificationPkg: String, notificationTitle: String?, notificationText: String?) {
        serviceScope.launch {
            try {
                val messageId = generateTimestampWithRandom()
                Log.d("NotificationService", "尝试保存通知，初始message_id: $messageId")
                
                val success = notificationRepository.saveNotification(
                    this@NotificationMonitorService,
                    notificationPkg,  // app_name
                    notificationTitle, // sender
                    notificationText, // content
                    messageId // message_id
                )
                
                if (success) {
                    Log.d("NotificationService", "通知保存成功")
                } else {
                    Log.e("NotificationService", "通知保存失败")
                }
            } catch (e: Exception) {
                Log.e("NotificationService", "保存通知时出错: ${e.message}", e)
            }
        }
    }

    override fun onNotificationPosted(sbn: StatusBarNotification) {
        super.onNotificationPosted(sbn)
        val notification = sbn.notification
        val extras = notification.extras
        val notificationPkg = sbn.packageName
        val notificationTitle = extras.getString(Notification.EXTRA_TITLE)
        val notificationText = extras.getString(Notification.EXTRA_TEXT)

        // 添加日志
        Log.d("NotificationService", "收到通知，包名: $notificationPkg")
        
        // 记录发出通知的包名
        NotificationPackages.addPackage(notificationPkg)

        if (isAppEnabled(notificationPkg) && notificationPkg != "com.tencent.mm") {
            handleNotification(notificationPkg, notificationTitle, notificationText)
        }

        if (notificationPkg == "com.tencent.mm") {
            Log.d("NotificationService", "收到通知：$notificationTitle - $notificationText")
            if (notificationTitle == "ASAP Sample" || notificationTitle == "备忘录") {
                handleNotification(notificationPkg, notificationTitle, notificationText)
            }
        } else if (notificationPkg == "com.ss.android.lark") {
            Log.d("NotificationService", "收到通知：$notificationTitle - $notificationText")
            if (notificationTitle == "ASAP Azure ToDoAgent") {
                handleNotification(notificationPkg, notificationTitle, notificationText)
            }
        }

        Log.d("收到的消息内容包名：", notificationPkg)
        //LogUtils.d("LogUtils:Notification posted $notificationTitle & $notificationText")
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

    override fun onCreate() {
        super.onCreate()
        Log.d("NotificationService", "服务已创建")
    }

    override fun onDestroy() {
        super.onDestroy()
        // 清理资源
        serviceScope.cancel()
        Log.d("NotificationService", "服务已销毁")
    }

    override fun onListenerConnected() {
        super.onListenerConnected()
        Log.d("NotificationService", "监听器已连接")
    }

    override fun onListenerDisconnected() {
        super.onListenerDisconnected()
        Log.d("NotificationService", "监听器已断开连接")
        // 尝试重新连接
        requestRebind(ComponentName(this, NotificationMonitorService::class.java))
    }
}





