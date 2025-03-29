package com.asap.todoexmple.receiver

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.SharedPreferences
import android.net.Uri
import android.os.Build
import android.os.PowerManager
import android.provider.Settings
import android.util.Log

object KeepAliveUtils {
    private const val PREFS_NAME = "keep_alive_settings"
    private const val KEY_BOOT_START = "boot_start_enabled"
    private const val KEY_BACKGROUND_START = "background_start_enabled"
    private const val KEY_HIDE_RECENTS = "hide_from_recents"
    private const val TAG = "KeepAliveUtils"

    private fun getPrefs(context: Context): SharedPreferences {
        return context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    }

    fun isBootStartEnabled(context: Context): Boolean {
        return getPrefs(context).getBoolean(KEY_BOOT_START, false)
    }

    fun setBootStartEnabled(context: Context, enabled: Boolean) {
        getPrefs(context).edit().putBoolean(KEY_BOOT_START, enabled).apply()
    }

    fun isBackgroundStartEnabled(context: Context): Boolean {
        return try {
            // 检查是否忽略电池优化
            val isIgnoringBatteryOptimizations = checkBatteryOptimization(context)
            
            // 检查自启动权限（根据不同厂商）
            val hasAutoStartPermission = checkManufacturerSpecificPermissions(context)
            
            Log.d(TAG, "电池优化: $isIgnoringBatteryOptimizations, 自启动权限: $hasAutoStartPermission")
            
            isIgnoringBatteryOptimizations && hasAutoStartPermission
        } catch (e: Exception) {
            Log.e(TAG, "检查后台启动权限时出错", e)
            false
        }
    }

    fun setBackgroundStartEnabled(context: Context, enabled: Boolean) {
        getPrefs(context).edit().putBoolean(KEY_BACKGROUND_START, enabled).apply()
    }

    fun isIgnoringBatteryOptimizations(context: Context): Boolean {
        return checkBatteryOptimization(context)
    }

    fun isHiddenFromRecents(context: Context): Boolean {
        return false // 默认不隐藏
    }

    fun setHiddenFromRecents(context: Context, hidden: Boolean) {
        getPrefs(context).edit().putBoolean(KEY_HIDE_RECENTS, hidden).apply()
    }

    fun startForegroundService(context: Context, serviceClass: Class<*>) {
        val intent = Intent(context, serviceClass)
        if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.O) {
            context.startForegroundService(intent)
        } else {
            context.startService(intent)
        }
    }

    fun restartService(context: Context, serviceClass: Class<*>) {
        context.stopService(Intent(context, serviceClass))
        startForegroundService(context, serviceClass)
    }

    fun requestIgnoreBatteryOptimizations(context: Context) {
        if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.M) {
            val intent = Intent()
            intent.action = android.provider.Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS
            intent.data = android.net.Uri.parse("package:${context.packageName}")
            intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            context.startActivity(intent)
        }
    }

    private fun checkBatteryOptimization(context: Context): Boolean {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            val powerManager = context.getSystemService(Context.POWER_SERVICE) as PowerManager
            powerManager.isIgnoringBatteryOptimizations(context.packageName)
        } else {
            true
        }
    }

    private fun checkManufacturerSpecificPermissions(context: Context): Boolean {
        return when (Build.MANUFACTURER.toLowerCase()) {
            "xiaomi" -> checkXiaomiBackgroundStart(context)
            "huawei" -> checkHuaweiBackgroundStart(context)
            "oppo" -> checkOppoBackgroundStart(context)
            "vivo" -> checkVivoBackgroundStart(context)
            else -> true
        }
    }

    private fun checkXiaomiBackgroundStart(context: Context): Boolean {
        try {
            // 小米手机检查自启动权限
            // 注意：实际上小米没有提供直接检查的API，这里只能通过间接方式
            return true
        } catch (e: Exception) {
            Log.e(TAG, "检查小米自启动权限失败", e)
            return false
        }
    }

    private fun checkHuaweiBackgroundStart(context: Context): Boolean {
        try {
            // 华为手机检查自启动权限
            // 注意：实际上华为没有提供直接检查的API，这里只能通过间接方式
            return true
        } catch (e: Exception) {
            Log.e(TAG, "检查华为自启动权限失败", e)
            return false
        }
    }

    private fun checkOppoBackgroundStart(context: Context): Boolean {
        try {
            // OPPO手机检查自启动权限
            // 注意：实际上OPPO没有提供直接检查的API，这里只能通过间接方式
            return true
        } catch (e: Exception) {
            Log.e(TAG, "检查OPPO自启动权限失败", e)
            return false
        }
    }

    private fun checkVivoBackgroundStart(context: Context): Boolean {
        try {
            // VIVO手机检查自启动权限
            // 注意：实际上VIVO没有提供直接检查的API，这里只能通过间接方式
            return true
        } catch (e: Exception) {
            Log.e(TAG, "检查VIVO自启动权限失败", e)
            return false
        }
    }

    // 跳转到对应品牌手机的自启动设置页面
    fun jumpToAutoStartSetting(context: Context) {
        try {
            val intent = when (Build.MANUFACTURER.toLowerCase()) {
                "xiaomi" -> Intent().apply {
                    component = ComponentName(
                        "com.miui.securitycenter",
                        "com.miui.permcenter.autostart.AutoStartManagementActivity"
                    )
                }
                "huawei" -> Intent().apply {
                    component = ComponentName(
                        "com.huawei.systemmanager",
                        "com.huawei.systemmanager.startupmgr.ui.StartupNormalAppListActivity"
                    )
                }
                "oppo" -> Intent().apply {
                    component = ComponentName(
                        "com.coloros.safecenter",
                        "com.coloros.safecenter.permission.startup.StartupAppListActivity"
                    )
                }
                "vivo" -> Intent().apply {
                    component = ComponentName(
                        "com.vivo.permissionmanager",
                        "com.vivo.permissionmanager.activity.BgStartUpManagerActivity"
                    )
                }
                else -> Intent(Settings.ACTION_SETTINGS)
            }
            intent.flags = Intent.FLAG_ACTIVITY_NEW_TASK
            context.startActivity(intent)
        } catch (e: Exception) {
            Log.e(TAG, "跳转到自启动设置页面失败", e)
            // 如果跳转失败，跳转到应用详情页面
            try {
                val intent = Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS).apply {
                    data = Uri.parse("package:${context.packageName}")
                    flags = Intent.FLAG_ACTIVITY_NEW_TASK
                }
                context.startActivity(intent)
            } catch (e: Exception) {
                Log.e(TAG, "跳转到应用详情页面也失败了", e)
            }
        }
    }

    // 跳转到电池优化设置页面
    fun jumpToBatteryOptimizationSetting(context: Context) {
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                val intent = Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS).apply {
                    data = Uri.parse("package:${context.packageName}")
                    flags = Intent.FLAG_ACTIVITY_NEW_TASK
                }
                context.startActivity(intent)
            }
        } catch (e: Exception) {
            Log.e(TAG, "跳转到电池优化设置页面失败", e)
        }
    }
} 