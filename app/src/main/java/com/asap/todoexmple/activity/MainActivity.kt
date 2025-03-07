package com.asap.todoexmple.activity

import android.Manifest
import android.annotation.SuppressLint
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.util.Log
import android.widget.TextView
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import com.asap.todoexmple.R
import com.asap.todoexmple.receiver.SmsReceiver
import com.asap.todoexmple.service.NotificationMonitorService
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class MainActivity : ComponentActivity() {
    private lateinit var tvSender: TextView
    private lateinit var tvContent: TextView
    private lateinit var tvDate: TextView
    private var isReceiverRegistered = false

    private val smsReceiver = object : BroadcastReceiver() {

        override fun onReceive(context: Context, intent: Intent) {
            if (intent?.action == SmsReceiver.SMS_RECEIVED_ACTION) {
                val sender = intent.getStringExtra("sender") ?: ""
                val body = intent.getStringExtra("body") ?: ""
                val timestamp = intent.getLongExtra("timestamp", 0L)

                updateSmsUI(sender, body, timestamp)
            }
        }

    }

    // 权限请求
    private val requestPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { permissions ->
        if (permissions.all { it.value }) {
            // 所有权限都已授予
            registerSmsReceiver()
        } else {
            Toast.makeText(this, "需要短信权限才能接收短信", Toast.LENGTH_SHORT).show()
        }
    }

    @SuppressLint("QueryPermissionsNeeded")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContentView(R.layout.activity_main)

        // 初始化视图
        tvSender = findViewById(R.id.tvSender)
        tvContent = findViewById(R.id.tvContent)
        tvDate = findViewById(R.id.tvDate)

        // 检查并请求权限
        checkAndRequestPermissions()

        if (NotificationManagerCompat.getEnabledListenerPackages(this).contains(packageName)){
            val intent = Intent(this, NotificationMonitorService::class.java)
            startService(intent)
        }else{
            startActivity(Intent("android.settings.ACTION_NOTIFICATION_LISTENER_SETTINGS"))
        }
    }

    private fun checkAndRequestPermissions() {
        val permissions = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            arrayOf(
                Manifest.permission.RECEIVE_SMS,
                Manifest.permission.READ_SMS,
                Manifest.permission.POST_NOTIFICATIONS
            )
        } else {
            arrayOf(
                Manifest.permission.RECEIVE_SMS,
                Manifest.permission.READ_SMS
            )
        }

        val permissionsToRequest = permissions.filter {
            ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED
        }.toTypedArray()

        if (permissionsToRequest.isEmpty()) {
            // 已经有所有需要的权限
            registerSmsReceiver()
        } else {
            // 请求权限
            requestPermissionLauncher.launch(permissionsToRequest)
        }
    }


    @SuppressLint("UnspecifiedRegisterReceiverFlag")
    private fun registerSmsReceiver() {
        if (!isReceiverRegistered) {
            val filter = IntentFilter(SmsReceiver.SMS_RECEIVED_ACTION)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                registerReceiver(smsReceiver, filter, Context.RECEIVER_NOT_EXPORTED)
            } else {
                registerReceiver(smsReceiver, filter)
            }
            isReceiverRegistered = true
        }
    }

    private fun updateSmsUI(sender: String, body: String, timestamp: Long) {
        tvSender.text = "发送人：$sender"
        tvContent.text = "短信内容：$body"
        Log.i(
            "发送人：$sender",
            " 短信内容：$body"
        )
        val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault())
        val date = Date(timestamp)
        tvDate.text = "时间：${dateFormat.format(date)}"
    }

    override fun onDestroy() {
        super.onDestroy()
        // 注销广播接收器
        if (isReceiverRegistered) {
            unregisterReceiver(smsReceiver)
            isReceiverRegistered = false
        }
    }
}