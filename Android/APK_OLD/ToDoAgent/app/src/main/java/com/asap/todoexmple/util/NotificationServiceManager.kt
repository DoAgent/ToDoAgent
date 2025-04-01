package com.asap.todoexmple.util

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.widget.Toast
import com.asap.todoexmple.service.NotificationMonitorService

class NotificationServiceManager(private val context: Context) {

    fun initNotificationService() {
        if (!isNotificationServiceEnabled()) {
            Toast.makeText(context, "请授权通知访问权限", Toast.LENGTH_LONG).show()
            context.startActivity(Intent("android.settings.ACTION_NOTIFICATION_LISTENER_SETTINGS"))
        } else {
            toggleNotificationListenerService()
        }
    }

    private fun toggleNotificationListenerService() {
        val thisComponent = ComponentName(context, NotificationMonitorService::class.java)
        context.packageManager.setComponentEnabledSetting(
            thisComponent,
            PackageManager.COMPONENT_ENABLED_STATE_DISABLED,
            PackageManager.DONT_KILL_APP
        )
        context.packageManager.setComponentEnabledSetting(
            thisComponent,
            PackageManager.COMPONENT_ENABLED_STATE_ENABLED,
            PackageManager.DONT_KILL_APP
        )

        val intent = Intent(context, NotificationMonitorService::class.java)
        context.stopService(intent)
        context.startService(intent)
    }

    private fun isNotificationServiceEnabled(): Boolean {
        val packageName = context.packageName
        val flat = android.provider.Settings.Secure.getString(
            context.contentResolver,
            "enabled_notification_listeners"
        )
        return flat?.contains(packageName) == true
    }
} 