package com.asap.todoexmple.service

import android.annotation.SuppressLint
import android.content.Context
import android.util.Log
import com.asap.todoexmple.application.SmsRepository
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class SmsHandler(private val context: Context) {
    private val smsRepository = SmsRepository()
    private val coroutineScope = CoroutineScope(Dispatchers.IO)

    fun handleSmsMessage(sender: String, body: String, timestamp: Long) {
        Log.d("SmsHandler", "开始处理短信")
        try {
            val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault())
            val date = Date(timestamp)
            val formattedDate = dateFormat.format(date)
            val messageId = generateTimestampWithRandom()

            // 保存到数据库
            coroutineScope.launch {
                Log.d("SmsHandler", "开始保存数据 - 发送人: $sender, 内容: $body, 收信时: $formattedDate")
                try {
                    val success = smsRepository.saveSmsData(context, sender, body, messageId)
                    Log.d("SmsHandler", if (success) "数据保存成功" else "数据保存失败")
                } catch (e: Exception) {
                    Log.e("SmsHandler", "保存数据时出错", e)
                }
            }
        } catch (e: Exception) {
            Log.e("SmsHandler", "处理短信时出错", e)
        }
    }

    companion object {
        @SuppressLint("StaticFieldLeak")
        @Volatile
        private var instance: SmsHandler? = null

        fun getInstance(context: Context): SmsHandler {
            return instance ?: synchronized(this) {
                instance ?: SmsHandler(context.applicationContext).also { instance = it }
            }
        }
    }
} 