package com.asap.todoexmple.activity

import android.annotation.SuppressLint
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.provider.Settings
import android.view.View
import android.app.ActivityManager
import android.os.Build
import androidx.annotation.RequiresApi
import androidx.appcompat.app.AppCompatActivity
import androidx.appcompat.widget.SwitchCompat
import com.asap.todoexmple.R
import com.asap.todoexmple.receiver.KeepAliveUtils
import android.widget.Toast
import android.widget.TextView
import android.widget.ImageView
import com.asap.todoexmple.util.UserManager
import com.asap.todoexmple.util.SessionManager
import com.asap.todoexmple.util.LocalDatabaseHelper
import android.content.pm.PackageManager


class SettingsActivity : AppCompatActivity() {
    // 添加 LocalDatabaseHelper 实例
    private lateinit var localDatabaseHelper: LocalDatabaseHelper

    companion object {
        private const val CALENDAR_PERMISSION_REQUEST_CODE = 1001
    }

    @SuppressLint("BatteryLife")
    @RequiresApi(Build.VERSION_CODES.TIRAMISU)
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_settings)

        // 初始化 LocalDatabaseHelper
        localDatabaseHelper = LocalDatabaseHelper(this)

        // 返回按钮点击事件
        findViewById<View>(R.id.btnBack).setOnClickListener {
            finish()
        }

        // 设置用户信息区域点击事件
        setupUserInfoSection()

        // 这里添加其他设置项的点击事件处理
        findViewById<View>(R.id.layoutNotification).setOnClickListener {
            // 处理通知设置点击
            startActivity(Intent(this, NotificationSettingsActivity::class.java))
        }

        findViewById<View>(R.id.layoutLanguage).setOnClickListener {
            // 处理语言设置点击
        }

        findViewById<View>(R.id.layoutHelp).setOnClickListener {
            //帮助中心
        }

        findViewById<View>(R.id.layoutAbout).setOnClickListener {
            // 处理关于我们点击
        }

        findViewById<View>(R.id.layoutLogout).setOnClickListener {
            // 退出登录点击
            try {
                // 调用SessionManager的登出方法
                SessionManager.Auth.logout(this)
                
                // 清除UserManager中的用户信息（使用正确的方法名）
                UserManager.clearUserInfo(this)
                
                // 重置界面状态
                setupUserInfoSection()
                
                // 显示退出成功提示
                Toast.makeText(this, "退出登录成功", Toast.LENGTH_SHORT).show()
                
                // 关闭保活相关功能
                KeepAliveUtils.setBootStartEnabled(this, false)
                KeepAliveUtils.setBackgroundStartEnabled(this, false)
                KeepAliveUtils.setHiddenFromRecents(this, false)
                
                // 重置所有开关状态
                findViewById<SwitchCompat>(R.id.switchBootStart).isChecked = false
                findViewById<SwitchCompat>(R.id.switchBatteryOptimization).isChecked = false
                findViewById<SwitchCompat>(R.id.switchHideRecents).isChecked = false
                
                // 清除所有Activity栈，并跳转到登录界面
                val intent = Intent(this, LoginActivity::class.java)
                intent.flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK
                startActivity(intent)
                finish()
            } catch (e: Exception) {
                Toast.makeText(this, "退出登录失败：${e.message}", Toast.LENGTH_SHORT).show()
            }
        }

        // 保活措施相关
        val layoutKeepAliveHeader = findViewById<View>(R.id.layoutKeepAliveHeader)
        val layoutKeepAliveOptions = findViewById<View>(R.id.layoutKeepAliveOptions)
        val imgKeepAliveArrow = findViewById<View>(R.id.imgKeepAliveArrow)
        
        // 开关控件
        val switchBootStart = findViewById<SwitchCompat>(R.id.switchBootStart)
        val switchBatteryOptimization = findViewById<SwitchCompat>(R.id.switchBatteryOptimization)
        val switchHideRecents = findViewById<SwitchCompat>(R.id.switchHideRecents)

        // 初始化开关状态
        switchBootStart.isChecked = KeepAliveUtils.isBootStartEnabled(this)
        switchBatteryOptimization.isChecked = KeepAliveUtils.isIgnoringBatteryOptimizations(this)
        switchHideRecents.isChecked = KeepAliveUtils.isHiddenFromRecents(this)

        // 点击展开/收起
        layoutKeepAliveHeader.setOnClickListener {
            if (layoutKeepAliveOptions.visibility == View.VISIBLE) {
                layoutKeepAliveOptions.visibility = View.GONE
                imgKeepAliveArrow.rotation = 0f
            } else {
                layoutKeepAliveOptions.visibility = View.VISIBLE
                imgKeepAliveArrow.rotation = 90f
            }
        }

        // 开关事件处理
        switchBootStart.setOnCheckedChangeListener { _, isChecked ->
            KeepAliveUtils.setBootStartEnabled(this, isChecked)
            KeepAliveUtils.setBackgroundStartEnabled(this, isChecked)
            if (isChecked) {
                Toast.makeText(this, "已开启开机自启动和后台自启动", Toast.LENGTH_SHORT).show()
            }
        }

        switchBatteryOptimization.setOnCheckedChangeListener { _, isChecked ->
            if (isChecked) {
                val intent = Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS).apply {
                    data = Uri.parse("package:$packageName")
                }
                startActivity(intent)
            }
        }

        switchHideRecents.setOnCheckedChangeListener { _, isChecked ->
            KeepAliveUtils.setHiddenFromRecents(this, isChecked)
            if (isChecked) {
                setTaskDescription(ActivityManager.TaskDescription.Builder()
                    .setLabel("")  // 空标签
                    .build())
            }
        }
    }

    private fun setupUserInfoSection() {
        val layoutUserInfo = findViewById<View>(R.id.layoutUserInfo)
        val txtLoginStatus = findViewById<TextView>(R.id.txtLoginStatus)
        val txtUserInfo = findViewById<TextView>(R.id.txtUserInfo)
        val imgAvatar = findViewById<ImageView>(R.id.imgAvatar)

        val isLoggedIn = UserManager.isLoggedIn(this)
        
        // 根据登录状态显示或隐藏退出登录按钮
        findViewById<View>(R.id.layoutLogout).visibility = 
            if (isLoggedIn) View.VISIBLE else View.GONE
        
        if (isLoggedIn) {
            txtLoginStatus.text = UserManager.getUserName(this)
            txtUserInfo.visibility = View.VISIBLE
            txtUserInfo.text = "ID: ${UserManager.getUserId(this)}"
        } else {
            txtLoginStatus.text = "登录/注册"
            txtUserInfo.visibility = View.GONE
        }

        layoutUserInfo.setOnClickListener {
            if (!isLoggedIn) {
                startActivity(Intent(this, LoginActivity::class.java))
            }
        }

        findViewById<View>(R.id.layoutAccountSecurity).visibility = 
            if (isLoggedIn) View.VISIBLE else View.GONE
    }



    private fun createCalendarReminder() {
        val todoListId = "1" // 替换为实际的 todoListId
        localDatabaseHelper.createCalendarReminder(this, todoListId)
    }

    override fun onRequestPermissionsResult(
        requestCode: Int,
        permissions: Array<out String>,
        grantResults: IntArray
    ) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        
        when (requestCode) {
            CALENDAR_PERMISSION_REQUEST_CODE -> {
                if (grantResults.isNotEmpty() && 
                    grantResults.all { it == PackageManager.PERMISSION_GRANTED }) {
                    // 用户授予了所有权限
                    createCalendarReminder()
                } else {
                    // 用户拒绝了权限
                    Toast.makeText(
                        this,
                        "需要日历权限才能创建提醒",
                        Toast.LENGTH_SHORT
                    ).show()
                }
            }
        }
    }

    // 在 Activity 销毁时关闭数据库连接
    override fun onDestroy() {
        super.onDestroy()
        localDatabaseHelper.close()
    }
} 