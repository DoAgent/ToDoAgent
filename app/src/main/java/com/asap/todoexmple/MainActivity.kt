package com.asap.todoexmple

import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.enableEdgeToEdge

import androidx.core.app.NotificationManagerCompat
import com.asap.todoexmple.service.NotificationMonitorService


class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        if (NotificationManagerCompat.getEnabledListenerPackages(this).contains(packageName)){
            val intent = Intent(this, NotificationMonitorService::class.java)
            startService(intent)
        }else{
            startActivity(Intent("android.settings.ACTION_NOTIFICATION_LISTENER_SETTINGS"))
        }

    }
}

