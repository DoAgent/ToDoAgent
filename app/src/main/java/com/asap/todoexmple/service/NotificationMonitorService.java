package com.asap.todoexmple.service;

import android.app.Notification;
import android.os.Bundle;
import android.service.notification.NotificationListenerService;
import android.service.notification.StatusBarNotification;
import android.util.Log;


public class NotificationMonitorService extends NotificationListenerService {
    @Override
    public void onNotificationPosted(StatusBarNotification sbn) {
        super.onNotificationPosted(sbn);
        Notification notification = sbn.getNotification();
        Bundle extras = notification.extras;

        // 获取包名
        String notificationPkg = sbn.getPackageName();
        // 获取通知标题（需处理API兼容性）
        String notificationTitle = extras.getString(Notification.EXTRA_TITLE);
        // 获取通知内容
        String notificationText = extras.getString(Notification.EXTRA_TEXT);

        Log.d("收到的消息内容包名：", notificationPkg);
        Log.d("收到的消息内容", "Notification posted " + notificationTitle + " & " + notificationText);
    }


}


