package com.asap.todoexmple.activity

import android.content.Context
import android.content.pm.ApplicationInfo
import android.content.pm.PackageManager
import android.os.Bundle
import android.view.View
import android.view.inputmethod.EditorInfo
import android.view.inputmethod.InputMethodManager
import android.widget.EditText
import android.widget.ImageView
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.asap.todoexmple.R
import com.asap.todoexmple.adapter.AppListAdapter
import com.asap.todoexmple.model.AppInfo
import android.text.Editable
import android.text.InputType
import android.text.TextWatcher
import android.view.KeyEvent
import android.content.Intent
import android.app.NotificationManager
import android.os.Build
import androidx.core.app.NotificationManagerCompat
import com.asap.todoexmple.service.NotificationPackages
import java.text.SimpleDateFormat
import java.util.*
import android.util.Log


class NotificationSettingsActivity : AppCompatActivity() {
    private lateinit var recyclerView: RecyclerView
    private lateinit var adapter: AppListAdapter
    private val appList = mutableListOf<AppInfo>()
    private val allAppList = mutableListOf<AppInfo>()

    // 定义观察者
    private val packageObserver: (Set<String>) -> Unit = { packages ->
        // 在主线程中更新UI
        runOnUiThread {
            loadInstalledApps()
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_notification_settings)

        // 设置返回按钮
        findViewById<View>(R.id.btnBack).setOnClickListener {
            finish()
        }

        // 初始化 RecyclerView
        recyclerView = findViewById(R.id.recyclerView)
        recyclerView.layoutManager = LinearLayoutManager(this)
        adapter = AppListAdapter(appList)
        recyclerView.adapter = adapter

        // 设置搜索框
        setupSearchView()

        // 加载已安装的应用列表
        loadInstalledApps()

        // 注册观察者
        NotificationPackages.addObserver(packageObserver)
    }

    override fun onDestroy() {
        super.onDestroy()
        // 移除观察者，避免内存泄漏
        NotificationPackages.removeObserver(packageObserver)
    }

    private fun setupSearchView() {
        val searchEditText = findViewById<EditText>(R.id.searchEditText)
        
        // 设置输入法动作
        searchEditText.imeOptions = EditorInfo.IME_ACTION_SEARCH
        searchEditText.inputType = InputType.TYPE_CLASS_TEXT
        
        // 处理输入法动作（回车键）
        searchEditText.setOnEditorActionListener { v, actionId, event ->
            if (actionId == EditorInfo.IME_ACTION_SEARCH || 
                (event?.keyCode == KeyEvent.KEYCODE_ENTER && event.action == KeyEvent.ACTION_DOWN)) {
                performSearch(v.text.toString())
                // 隐藏键盘
                val imm = getSystemService(Context.INPUT_METHOD_SERVICE) as InputMethodManager
                imm.hideSoftInputFromWindow(v.windowToken, 0)
                return@setOnEditorActionListener true
            }
            false
        }

        // 实时搜索（可选）
        searchEditText.addTextChangedListener(object : TextWatcher {
            override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) {}
            override fun onTextChanged(s: CharSequence?, start: Int, before: Int, count: Int) {}
            override fun afterTextChanged(s: Editable?) {
                performSearch(s.toString())
            }
        })
    }

    private fun performSearch(query: String) {
        if (query.isEmpty()) {
            // 如果搜索框为空，显示所有应用
            appList.clear()
            appList.addAll(allAppList)
        } else {
            // 根据输入过滤应用
            appList.clear()
            appList.addAll(allAppList.filter { app ->
                app.appName.contains(query, ignoreCase = true) ||
                app.packageName.contains(query, ignoreCase = true)
            })
        }
        adapter.notifyDataSetChanged()
    }

    private fun loadInstalledApps() {
        val packageManager = packageManager
        val notificationPackages = NotificationPackages.getAllPackages()
        
        allAppList.clear()
        
        // 打印日志查看包名
        Log.d("NotificationSettings", "所有通知包: ${notificationPackages.joinToString()}")
        
        notificationPackages.forEach { packageName ->
            try {
                // 直接通过包名获取应用信息
                val appInfo = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                    packageManager.getApplicationInfo(packageName, PackageManager.ApplicationInfoFlags.of(0))
                } else {
                    packageManager.getApplicationInfo(packageName, 0)
                }
                
                val isSystemApp = (appInfo.flags and ApplicationInfo.FLAG_SYSTEM) != 0
                val isUpdatedSystemApp = (appInfo.flags and ApplicationInfo.FLAG_UPDATED_SYSTEM_APP) != 0
                
                // 获取应用图标（如果获取失败则使用默认图标）
                val appIcon = try {
                    appInfo.loadIcon(packageManager)
                } catch (e: Exception) {
                    packageManager.defaultActivityIcon
                }
                
                // 获取应用名称（如果获取失败则使用包名）
                val appLabel = try {
                    appInfo.loadLabel(packageManager).toString()
                } catch (e: Exception) {
                    packageName
                }
                
                allAppList.add(
                    AppInfo(
                        appName = buildString {
                            append(appLabel)
                            if (isSystemApp) append(" [系统]")
                            if (isUpdatedSystemApp) append(" [更新]")
                            append("\n")
                            append(packageName)
                            append("\n[记录时间: ${getCurrentTime()}]")
                        },
                        packageName = packageName,
                        appIcon = appIcon
                    )
                )
                
                Log.d("NotificationSettings", "成功添加应用: $packageName")
                
            } catch (e: Exception) {
                Log.e("NotificationSettings", "获取应用信息失败: $packageName", e)
                // 如果获取应用信息失败，仍然添加到列表中
                allAppList.add(
                    AppInfo(
                        appName = "未知应用\n$packageName\n[记录时间: ${getCurrentTime()}]",
                        packageName = packageName,
                        appIcon = packageManager.defaultActivityIcon
                    )
                )
            }
        }

        // 按时间倒序排序
        allAppList.sortByDescending { it.packageName }
        
        // 更新显示列表
        appList.clear()
        appList.addAll(allAppList)
        adapter.notifyDataSetChanged()
        
        // 更新统计信息
        updateStatistics()
        
        // 打印当前列表
        Log.d("NotificationSettings", "当前显示的应用列表: ${appList.map { it.packageName }.joinToString()}")
    }

    private fun getCurrentTime(): String {
        return SimpleDateFormat("HH:mm:ss", Locale.getDefault()).format(Date())
    }

    private fun updateStatistics() {
        val totalApps = allAppList.size
        findViewById<TextView>(R.id.statisticsText)?.text = 
            "已发现 $totalApps 个发送过通知的应用 (最后更新: ${getCurrentTime()})"
    }
} 