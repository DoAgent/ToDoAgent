 package com.asap.todoexmple.activity

import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import android.widget.Button
import android.widget.EditText
import android.widget.ImageButton
import android.widget.TextView
import com.asap.todoexmple.R

class TaskDetailActivity : AppCompatActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_task_detail)

        // 返回按钮点击事件
        findViewById<ImageButton>(R.id.backButton).setOnClickListener {
            finish()
        }

        // 保存按钮点击事件
        findViewById<Button>(R.id.saveButton).setOnClickListener {
            saveTaskChanges()
        }

        // 删除按钮点击事件
        findViewById<ImageButton>(R.id.deleteButton).setOnClickListener {
            deleteTask()
        }

        // 分享按钮点击事件
        findViewById<ImageButton>(R.id.shareButton).setOnClickListener {
            shareTask()
        }
    }

    private fun saveTaskChanges() {
        // 实现保存任务更改的逻辑
    }

    private fun deleteTask() {
        // 实现删除任务的逻辑
    }

    private fun shareTask() {
        // 实现分享任务的逻辑
    }
}