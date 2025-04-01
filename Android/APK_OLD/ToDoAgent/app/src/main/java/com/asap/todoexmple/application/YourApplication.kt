package com.asap.todoexmple.application

import android.annotation.SuppressLint
import android.app.Application
import android.content.Intent
import android.net.Uri
import android.os.PowerManager
import android.provider.Settings
import android.util.Log

import com.asap.todoexmple.service.ForegroundService
import com.asap.todoexmple.util.DatabaseHelper

class YourApplication : Application() {
    lateinit var smsViewModel: SmsViewModel
        private set

    override fun onCreate() {
        super.onCreate()
        instance = this  // 设置实例

        // 初始化 SmsViewModel
        smsViewModel = SmsViewModel(this)

        // 初始化数据库
        DatabaseHelper.initialize(this)

        // 请求必要权限
        requestPermissions()

        // 启动前台服务
        startForegroundService()
    }

    @SuppressLint("BatteryLife")
    private fun requestPermissions() {
        // 请求忽略电池优化
        if (!isIgnoringBatteryOptimizations()) {
            try {
                val intent = Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS).apply {
                    data = Uri.parse("package:$packageName")
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                startActivity(intent)
            } catch (e: Exception) {
                Log.e("App", "请求电池优化失败", e)
            }
        }
    }

    private fun isIgnoringBatteryOptimizations(): Boolean {
        val powerManager = getSystemService(POWER_SERVICE) as PowerManager
        return powerManager.isIgnoringBatteryOptimizations(packageName)
    }

    private fun startForegroundService() {
        val serviceIntent = Intent(this, ForegroundService::class.java)
        startForegroundService(serviceIntent)
    }

    override fun onTerminate() {
        super.onTerminate()
        DatabaseHelper.closeAllConnections()
    }

    companion object {
        @Volatile
        private var instance: YourApplication? = null

        fun getInstance(): YourApplication {
            return instance ?: throw IllegalStateException("Application not created yet!")
        }
    }
}