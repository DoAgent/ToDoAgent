package com.asap.todoexmple.application

import android.annotation.SuppressLint
import android.content.Context
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.ViewModelProvider
import android.app.Application
import com.asap.todoexmple.service.SmsHandler
import com.asap.todoexmple.receiver.KeepAliveUtils
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import android.util.Log

data class SmsMessage(
    val sender: String,
    val body: String,
    val timestamp: Long
)

// 将 ViewModel 改为 AndroidViewModel
class SmsViewModel(application: Application) : AndroidViewModel(application) {
    private val _smsFlow = MutableSharedFlow<SmsMessage>()
    val smsFlow = _smsFlow.asSharedFlow()
    private val smsHandler = SmsHandler.getInstance(application.applicationContext)
    @SuppressLint("StaticFieldLeak")
    private val context: Context = application.applicationContext

    suspend fun sendSms(sender: String, body: String, timestamp: Long) {
        try {
            Log.d("SmsViewModel", "开始处理短信: sender=$sender")

            // 移除后台自启动检查，直接处理短信
            _smsFlow.emit(SmsMessage(sender, body, timestamp))
            smsHandler.handleSmsMessage(sender, body, timestamp)

            // 如果后台自启动未启用，仅记录警告日志
            if (!KeepAliveUtils.isBackgroundStartEnabled(context)) {
                Log.w("SmsViewModel", "警告：后台自启动未启用，可能影响应用在后台的运行")
            }

            Log.d("SmsViewModel", "短信处理完成")
        } catch (e: Exception) {
            Log.e("SmsViewModel", "处理短信失败: ${e.message}", e)
            throw e
        }
    }

    // 添加 ViewModelFactory
    class Factory(private val application: Application) : ViewModelProvider.Factory {
        override fun <T : androidx.lifecycle.ViewModel> create(modelClass: Class<T>): T {
            if (modelClass.isAssignableFrom(SmsViewModel::class.java)) {
                @Suppress("UNCHECKED_CAST")
                return SmsViewModel(application) as T
            }
            throw IllegalArgumentException("Unknown ViewModel class")
        }
    }
}