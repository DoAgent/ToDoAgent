package com.asap.todoexmple.fragment

import Task
import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.os.Bundle
import android.util.Log
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.Button
import android.widget.Toast
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import androidx.fragment.app.Fragment
import androidx.lifecycle.lifecycleScope
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.asap.todoexmple.R

import com.asap.todoexmple.adapter.TaskAdapter
import com.asap.todoexmple.util.LocalDatabaseHelper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class HomeFragment : Fragment() {
    // 将按钮声明为可空类型，避免 lateinit 初始化问题
    private var btnAll: Button? = null
    private var btnToday: Button? = null
    private var btnImportant: Button? = null
    private var btnCompleted: Button? = null
    private var taskList: RecyclerView? = null
    private var taskAdapter: TaskAdapter? = null
    private lateinit var dbHelper: LocalDatabaseHelper

    private var pendingTaskId: String? = null

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View? {
        return inflater.inflate(R.layout.fragment_home, container, false)
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)
        dbHelper = LocalDatabaseHelper(requireContext())
        initViews(view)
        setupTaskList()
        setupCategoryButtons()
        // 默认选中"今天"按钮
        btnToday?.let { button ->
            resetButtonStyles(listOfNotNull(btnAll, btnToday, btnImportant, btnCompleted))
            setSelectedButtonStyle(button)
            loadTodayTasks() // 加载今天的任务
        }
    }

    private fun initViews(view: View) {
        btnAll = view.findViewById(R.id.btnAll)
        btnToday = view.findViewById(R.id.btnToday)
        btnImportant = view.findViewById(R.id.btnImportant)
        btnCompleted = view.findViewById(R.id.btnCompleted)
        taskList = view.findViewById(R.id.taskList)
    }

    private fun setupTaskList() {
        taskAdapter = TaskAdapter(
            onImportantClick = { task, isImportant ->
                updateTaskImportance(task.listId, isImportant)
            },
            onCompletedClick = { task, isCompleted ->
                updateTaskCompletion(task.listId, isCompleted)
            },
            onCalendarClick = { task ->
                checkAndRequestCalendarPermissions(task.listId)
            }
        )
        taskList?.apply {
            layoutManager = LinearLayoutManager(context)
            adapter = taskAdapter
        }
    }

    private fun setupCategoryButtons() {
        val buttons = listOfNotNull(btnAll, btnToday, btnImportant, btnCompleted)
        buttons.forEach { button ->
            button.setOnClickListener {
                resetButtonStyles(buttons)
                setSelectedButtonStyle(button)
                handleButtonClick(button)
            }
        }
    }

    private fun resetButtonStyles(buttons: List<Button>) {
        buttons.forEach { button ->
            //button.setBackgroundResource(R.drawable.`button_rounded.xml`)
            button.setTextColor(requireContext().getColor(android.R.color.darker_gray))
            button.backgroundTintList = android.content.res.ColorStateList.valueOf(
                requireContext().getColor(android.R.color.white)
            )
        }
    }

    private fun setSelectedButtonStyle(button: Button) {
        button.backgroundTintList = android.content.res.ColorStateList.valueOf(
            requireContext().getColor(R.color.blue_500) // 使用统一的蓝色
        )
        button.setTextColor(requireContext().getColor(android.R.color.white))
    }

    private fun handleButtonClick(button: Button) {
        when (button.id) {
            R.id.btnAll -> loadAllTasks()
            R.id.btnToday -> loadTodayTasks()
            R.id.btnImportant -> loadImportantTasks()
            R.id.btnCompleted -> loadCompletedTasks()
        }
    }

    private fun loadAllTasks() {
        lifecycleScope.launch(Dispatchers.IO) {
            val cursor = dbHelper.readableDatabase.query(
                "ToDoListLocal",
                null,
                null,
                null,
                null,
                null,
                "start_time DESC"
            )
            
            val tasks = mutableListOf<Task>()
            cursor.use {
                while (it.moveToNext()) {
                    tasks.add(cursorToTask(it))
                }
            }
            
            withContext(Dispatchers.Main) {
                taskAdapter?.updateTasks(tasks)
            }
        }
    }

    private fun loadTodayTasks() {
        lifecycleScope.launch(Dispatchers.IO) {
            val today = java.time.LocalDate.now().toString()
            val cursor = dbHelper.readableDatabase.query(
                "ToDoListLocal",
                null,
                "date(start_time) = ?",
                arrayOf(today),
                null,
                null,
                "start_time ASC"
            )
            
            val tasks = mutableListOf<Task>()
            cursor.use {
                while (it.moveToNext()) {
                    tasks.add(cursorToTask(it))
                }
            }
            
            withContext(Dispatchers.Main) {
                taskAdapter?.updateTasks(tasks)
            }
        }
    }

    private fun loadImportantTasks() {
        lifecycleScope.launch(Dispatchers.IO) {
            try {
                // 首先检查列是否存在
                val cursor = dbHelper.readableDatabase.rawQuery(
                    "SELECT * FROM ToDoListLocal LIMIT 1", null
                )
                val columnNames = cursor.columnNames.toList()
                cursor.close()

                val hasImportantColumn = columnNames.contains("is_important")
                
                // 根据列是否存在使用不同的查询
                val query = if (hasImportantColumn) {
                    dbHelper.readableDatabase.query(
                        "ToDoListLocal",
                        null,
                        "is_important = ?",
                        arrayOf("1"),
                        null,
                        null,
                        "start_time DESC"
                    )
                } else {
                    // 如果列不存在，返回空结果
                    dbHelper.readableDatabase.query(
                        "ToDoListLocal",
                        null,
                        "1 = 0", // 始终为false的条件
                        null,
                        null,
                        null,
                        "start_time DESC"
                    )
                }

                val tasks = mutableListOf<Task>()
                query.use {
                    while (it.moveToNext()) {
                        tasks.add(cursorToTask(it))
                    }
                }

                withContext(Dispatchers.Main) {
                    taskAdapter?.updateTasks(tasks)
                }
            } catch (e: Exception) {
                Log.e("HomeFragment", "加载重要任务失败", e)
                withContext(Dispatchers.Main) {
                    taskAdapter?.updateTasks(emptyList())
                }
            }
        }
    }

    private fun loadCompletedTasks() {
        lifecycleScope.launch(Dispatchers.IO) {
            val cursor = dbHelper.readableDatabase.query(
                "ToDoListLocal",
                null,
                "is_completed = ?",
                arrayOf("1"),
                null,
                null,
                "start_time DESC"
            )
            
            val tasks = mutableListOf<Task>()
            cursor.use {
                while (it.moveToNext()) {
                    tasks.add(cursorToTask(it))
                }
            }
            
            withContext(Dispatchers.Main) {
                taskAdapter?.updateTasks(tasks)
            }
        }
    }

    private fun updateTaskImportance(taskId: String, isImportant: Boolean) {
        lifecycleScope.launch(Dispatchers.IO) {
            try {
                dbHelper.writableDatabase.use { db ->
                    val values = android.content.ContentValues().apply {
                        put("is_important", if (isImportant) 1 else 0)
                        put("last_modified", System.currentTimeMillis().toString())
                        put("sync_status", 0) // 标记需要同步
                    }
                    
                    db.update(
                        "ToDoListLocal",
                        values,
                        "list_id = ?",
                        arrayOf(taskId)
                    )
                }
                
                // 刷新当前显示的任务列表
                when (getCurrentSelectedButton()) {
                    R.id.btnAll -> loadAllTasks()
                    R.id.btnToday -> loadTodayTasks()
                    R.id.btnImportant -> loadImportantTasks()
                    R.id.btnCompleted -> loadCompletedTasks()
                }
                
                // 触发立即同步
                val userId = getUserId() // 需要实现获取用户ID的方法
                if (userId != null) {
                    LocalDatabaseHelper.startImmediateSync(requireContext(), userId)
                }
            } catch (e: Exception) {
                Log.e("HomeFragment", "更新任务重要性失败", e)
            }
        }
    }

    private fun updateTaskCompletion(taskId: String, isCompleted: Boolean) {
        lifecycleScope.launch(Dispatchers.IO) {
            try {
                dbHelper.writableDatabase.use { db ->
                    val values = android.content.ContentValues().apply {
                        put("is_completed", if (isCompleted) 1 else 0)
                        put("last_modified", System.currentTimeMillis().toString())
                        put("sync_status", 0) // 标记需要同步
                    }
                    
                    db.update(
                        "ToDoListLocal",
                        values,
                        "list_id = ?",
                        arrayOf(taskId)
                    )
                }
                
                // 刷新当前显示的任务列表
                when (getCurrentSelectedButton()) {
                    R.id.btnAll -> loadAllTasks()
                    R.id.btnToday -> loadTodayTasks()
                    R.id.btnImportant -> loadImportantTasks()
                    R.id.btnCompleted -> loadCompletedTasks()
                }
                
                // 触发立即同步
                val userId = getUserId()
                if (userId != null) {
                    LocalDatabaseHelper.startImmediateSync(requireContext(), userId)
                }
            } catch (e: Exception) {
                Log.e("HomeFragment", "更新任务完成状态失败", e)
            }
        }
    }

    private fun getCurrentSelectedButton(): Int {
        return when {
            btnAll?.backgroundTintList?.defaultColor == requireContext().getColor(R.color.blue_500) -> R.id.btnAll
            btnToday?.backgroundTintList?.defaultColor == requireContext().getColor(R.color.blue_500) -> R.id.btnToday
            btnImportant?.backgroundTintList?.defaultColor == requireContext().getColor(R.color.blue_500) -> R.id.btnImportant
            btnCompleted?.backgroundTintList?.defaultColor == requireContext().getColor(R.color.blue_500) -> R.id.btnCompleted
            else -> R.id.btnAll
        }
    }

    private fun getUserId(): String? {
        return context?.getSharedPreferences("user_prefs", Context.MODE_PRIVATE)
            ?.getString("current_user_id", null)
    }

    private fun cursorToTask(cursor: android.database.Cursor): Task {
        return Task(
            listId = cursor.getString(cursor.getColumnIndexOrThrow("list_id")),
            userId = cursor.getString(cursor.getColumnIndexOrThrow("user_id")),
            startTime = cursor.getString(cursor.getColumnIndexOrThrow("start_time")),
            endTime = cursor.getString(cursor.getColumnIndexOrThrow("end_time")),
            location = cursor.getString(cursor.getColumnIndexOrThrow("location")),
            content = cursor.getString(cursor.getColumnIndexOrThrow("todo_content")),
            isImportant = cursor.getInt(cursor.getColumnIndexOrThrow("is_important")) == 1,
            isCompleted = cursor.getInt(cursor.getColumnIndexOrThrow("is_completed")) == 1,
            syncStatus = cursor.getInt(cursor.getColumnIndexOrThrow("sync_status")),
            lastModified = cursor.getString(cursor.getColumnIndexOrThrow("last_modified"))
        )
    }

    // 添加日历权限检查和请求方法
    private fun checkAndRequestCalendarPermissions(taskId: String) {
        val readPermission = ContextCompat.checkSelfPermission(
            requireContext(), 
            Manifest.permission.READ_CALENDAR
        )
        val writePermission = ContextCompat.checkSelfPermission(
            requireContext(), 
            Manifest.permission.WRITE_CALENDAR
        )

        val permissionsToRequest = mutableListOf<String>()
        
        if (readPermission != PackageManager.PERMISSION_GRANTED) {
            permissionsToRequest.add(Manifest.permission.READ_CALENDAR)
        }
        if (writePermission != PackageManager.PERMISSION_GRANTED) {
            permissionsToRequest.add(Manifest.permission.WRITE_CALENDAR)
        }

        if (permissionsToRequest.isNotEmpty()) {
            requestPermissions(
                permissionsToRequest.toTypedArray(),
                CALENDAR_PERMISSION_REQUEST_CODE
            )
            // 保存当前任务ID，以便在权限授予后使用
            pendingTaskId = taskId
        } else {
            // 已有权限，直接创建日历提醒
            createCalendarReminder(taskId)
        }
    }

    companion object {
        private const val CALENDAR_PERMISSION_REQUEST_CODE = 1001
    }

    override fun onRequestPermissionsResult(
        requestCode: Int,
        permissions: Array<out String>,
        grantResults: IntArray
    ) {
        when (requestCode) {
            CALENDAR_PERMISSION_REQUEST_CODE -> {
                if (grantResults.isNotEmpty() && 
                    grantResults.all { it == PackageManager.PERMISSION_GRANTED }) {
                    // 用户授予了所有权限
                    pendingTaskId?.let { taskId ->
                        createCalendarReminder(taskId)
                    }
                } else {
                    // 用户拒绝了权限
                    Toast.makeText(
                        requireContext(),
                        "需要日历权限才能创建提醒",
                        Toast.LENGTH_SHORT
                    ).show()
                }
                pendingTaskId = null
            }
            else -> super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        }
    }

    private fun createCalendarReminder(taskId: String) {
        dbHelper.createCalendarReminder(requireContext(), taskId)
        Toast.makeText(
            requireContext(),
            "已添加到日历提醒",
            Toast.LENGTH_SHORT
        ).show()
    }
} 