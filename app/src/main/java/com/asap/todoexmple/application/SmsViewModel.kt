package com.asap.todoexmple.application

import androidx.lifecycle.ViewModel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow

data class SmsMessage(
    val sender: String,
    val body: String,
    val timestamp: Long
)

class SmsViewModel : ViewModel() {
    private val _smsFlow = MutableSharedFlow<SmsMessage>()
    val smsFlow = _smsFlow.asSharedFlow()

    suspend fun sendSms(sender: String, body: String, timestamp: Long) {
        _smsFlow.emit(SmsMessage(sender, body, timestamp))
    }
}
