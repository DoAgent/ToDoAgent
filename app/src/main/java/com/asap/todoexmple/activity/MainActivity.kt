package com.asap.todoexmple.activity

import android.Manifest
import android.annotation.SuppressLint
import android.content.ComponentName
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.util.Log
import android.widget.TextView
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.annotation.RequiresApi
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.asap.todoexmple.R
import com.asap.todoexmple.application.SmsRepository
import com.asap.todoexmple.application.SmsViewModel
import com.asap.todoexmple.application.YourApplication

import com.asap.todoexmple.service.NotificationMonitorService
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class MainActivity : ComponentActivity() {
    private lateinit var tvSender: TextView
    private lateinit var tvContent: TextView
    private lateinit var tvDate: TextView
    private lateinit var smsViewModel: SmsViewModel
    private val smsRepository = SmsRepository()

    // 权限请求
    @RequiresApi(Build.VERSION_CODES.TIRAMISU)
    private val requestPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { permissions ->
        if (!permissions.all { it.value }) {
            Toast.makeText(this, "需要短信权限才能接收短信", Toast.LENGTH_SHORT).show()
        }
    }

/////////////////android 生命周期之onCreat//////////////////////////
   @RequiresApi(Build.VERSION_CODES.TIRAMISU)
   override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        setContentView(R.layout.activity_main)

        // 通知监听服务初始化
        initNotificationService()

        // 初始化视图
        initViews()

        // 获取 ViewModel
        smsViewModel = (application as YourApplication).smsViewModel

        // 检查并请求权限
        checkAndRequestPermissions()

        // 观察短信更新
        observeSmsUpdates()
    }
/////////////////自定义的函数//////////////////////////
    private fun observeSmsUpdates() {
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                smsViewModel.smsFlow.collect { smsMessage ->
                    updateSmsUI(smsMessage.sender, smsMessage.body, smsMessage.timestamp)
                }
            }
        }
    }

    private fun initViews() {
        tvSender = findViewById(R.id.tvSender)
        tvContent = findViewById(R.id.tvContent)
        tvDate = findViewById(R.id.tvDate)
    }

    //////通知获取的函数小伙伴们/////
    private fun initNotificationService() {
        // 修改通知监听服务的启动逻辑
        if (!isNotificationServiceEnabled()) {
            // 如果服务未启用，引导用户去设置
            Toast.makeText(this, "请授权通知访问权限", Toast.LENGTH_LONG).show()
            startActivity(Intent("android.settings.ACTION_NOTIFICATION_LISTENER_SETTINGS"))
        } else {
            // 服务已启用，确保服务正在运行
            toggleNotificationListenerService()
        }
    }

    // 添加切换服务的方法
    private fun toggleNotificationListenerService() {
        val thisComponent = ComponentName(this, NotificationMonitorService::class.java)
        packageManager.setComponentEnabledSetting(
            thisComponent,
            PackageManager.COMPONENT_ENABLED_STATE_DISABLED,
            PackageManager.DONT_KILL_APP
        )
        packageManager.setComponentEnabledSetting(
            thisComponent,
            PackageManager.COMPONENT_ENABLED_STATE_ENABLED,
            PackageManager.DONT_KILL_APP
        )

        // 重启服务
        val intent = Intent(this, NotificationMonitorService::class.java)
        stopService(intent)
        startService(intent)
    }

    // 添加检查通知监听服务是否启用的方法
    private fun isNotificationServiceEnabled(): Boolean {
        val packageNames = NotificationManagerCompat.getEnabledListenerPackages(this)
        return packageNames.contains(packageName)
    }
    //////通知获取的函数小伙伴们--到这里结束啦/////

    @RequiresApi(Build.VERSION_CODES.TIRAMISU)
    private fun checkAndRequestPermissions() {
        val permissions =
            arrayOf(
                Manifest.permission.RECEIVE_SMS,
                Manifest.permission.READ_SMS,
                Manifest.permission.POST_NOTIFICATIONS
            )

        val permissionsToRequest = permissions.filter {
            ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED
        }.toTypedArray()

        if (permissionsToRequest.isNotEmpty()) {
            requestPermissionLauncher.launch(permissionsToRequest)
        }
    }

    @SuppressLint("SetTextI18n")
    private fun updateSmsUI(sender: String, body: String, timestamp: Long) {
        Log.d("MainActivity", "开始更新UI")
        tvSender.text = "发送人：$sender"
        tvContent.text = "短信内容：$body"
        try {
            val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault())
            val date = Date(timestamp)
            val formattedDate = dateFormat.format(date)
            tvDate.text = "时间：$formattedDate"
            Log.d("MainActivity", "UI 已更新 - 发送人: $sender, 内容: $body")
            // 保存到数据库
            lifecycleScope.launch(Dispatchers.IO) {
                Log.d("MainActivity", "开始保存数据")
                val success = try {
                    smsRepository.saveSmsData(sender, body)
                } catch (e: Exception) {
                    Log.e("MainActivity", "保存过程出错", e)
                    false
                }

                withContext(Dispatchers.Main) {
                    val message = if (success) "保存成功" else "保存失败"
                    Toast.makeText(this@MainActivity, message, Toast.LENGTH_SHORT).show()
                    Log.d("MainActivity", "保存结果: $message")
                }
            }
                } catch (e: Exception) {
                    Log.e("MainActivity", "更新 UI 时出错", e)
                }
            }
        }



