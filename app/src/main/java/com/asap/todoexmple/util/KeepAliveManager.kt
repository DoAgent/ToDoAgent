package com.asap.todoexmple.util

import android.app.AlertDialog
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.PowerManager
import android.provider.Settings
import android.util.Log
import android.widget.Toast
import com.asap.todoexmple.receiver.KeepAliveUtils

class KeepAliveManager(private val context: Context) {

    fun initKeepAlive() {
        if (!KeepAliveUtils.isIgnoringBatteryOptimizations(context)) {
            val intent = Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS).apply {
                data = Uri.parse("package:${context.packageName}")
            }
            context.startActivity(intent)
        }
    }

    fun checkBackgroundStartPermission() {
        if (!KeepAliveUtils.isBackgroundStartEnabled(context)) {
            AlertDialog.Builder(context)
                .setTitle("需要权限")
                .setMessage("请允许应用在后台运行，以确保短信正常处理")
                .setPositiveButton("去设置") { _, _ ->
                    try {
                        val intent = Intent().apply {
                            action = Settings.ACTION_APPLICATION_DETAILS_SETTINGS
                            data = Uri.fromParts("package", context.packageName, null)
                        }
                        context.startActivity(intent)
                    } catch (e: Exception) {
                        Log.e("KeepAliveManager", "打开设置页面失败", e)
                        Toast.makeText(context, "请手动开启应用的后台自启动权限", Toast.LENGTH_LONG).show()
                    }
                }
                .setNegativeButton("取消", null)
                .show()
        }
    }

    fun requestBatteryOptimizationPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            val powerManager = context.getSystemService(Context.POWER_SERVICE) as PowerManager
            if (!powerManager.isIgnoringBatteryOptimizations(context.packageName)) {
                try {
                    val intent = Intent().apply {
                        action = Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS
                        data = Uri.parse("package:${context.packageName}")
                    }
                    context.startActivity(intent)
                } catch (e: Exception) {
                    Log.e("KeepAliveManager", "请求电池优化白名单失败", e)
                }
            }
        }
    }

    fun checkAndRequestBackgroundPermissions() {
        if (!KeepAliveUtils.isBackgroundStartEnabled(context)) {
            AlertDialog.Builder(context)
                .setTitle("需要权限")
                .setMessage("为了确保应用正常运行，请允许应用自启动和后台运行权限")
                .setPositiveButton("去设置") { _, _ ->
                    KeepAliveUtils.jumpToAutoStartSetting(context)
                    KeepAliveUtils.jumpToBatteryOptimizationSetting(context)
                }
                .setNegativeButton("取消") { _, _ ->
                    Toast.makeText(context, "未授予权限可能会影响应用正常运行", Toast.LENGTH_LONG).show()
                }
                .show()
        }
    }
} 