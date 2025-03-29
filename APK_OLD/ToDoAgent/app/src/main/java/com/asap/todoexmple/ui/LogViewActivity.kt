package com.asap.todoexmple.ui

import android.os.Bundle
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import java.io.File
import kotlinx.coroutines.*

class LogViewActivity : AppCompatActivity() {
    private lateinit var logTextView: TextView
    private val scope = CoroutineScope(Dispatchers.Main + Job())
    private var updateJob: Job? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        
        logTextView = TextView(this).apply {
            setPadding(16, 16, 16, 16)
        }
        setContentView(logTextView)

        startLogUpdates()
    }

    private fun startLogUpdates() {
        updateJob = scope.launch {
            while (isActive) {
                updateLogDisplay()
                delay(1000) // 每秒更新一次
            }
        }
    }

    private fun updateLogDisplay() {
        val logDir = File(filesDir, "logs")
        val files = logDir.listFiles()?.filter { it.name.endsWith(".txt") }
        
        files?.maxByOrNull { it.lastModified() }?.let { latestLog ->
            val content = latestLog.readText()
            logTextView.text = content
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        updateJob?.cancel()
        scope.cancel()
    }
} 