package com.asap.todoexmple.receiver
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.telephony.SmsMessage
import android.util.Log
import com.asap.todoexmple.application.YourApplication
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch

class SmsReceiver : BroadcastReceiver() {
    companion object {
        const val SMS_RECEIVED_ACTION = "android.provider.Telephony.SMS_RECEIVED"
    }

    @OptIn(DelicateCoroutinesApi::class)
    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != SMS_RECEIVED_ACTION) return

        val bundle = intent.extras ?: return
        val pdus = bundle["pdus"] as? Array<*> ?: return

        var sender = ""
        val fullMessage = StringBuilder()
        var timestamp = 0L

        pdus.forEach { pdu ->
            val message = SmsMessage.createFromPdu(pdu as ByteArray)
            if (sender.isEmpty()) {
                sender = message.displayOriginatingAddress
                timestamp = message.timestampMillis
            }
            fullMessage.append(message.displayMessageBody)
        }

        Log.d("SMS", "来自：$sender，内容：$fullMessage")

        // 使用 Application 实例访问 ViewModel
        GlobalScope.launch {
            try {
                YourApplication.getInstance().smsViewModel.sendSms(
                    sender,
                    fullMessage.toString(),
                    timestamp
                )
            } catch (e: Exception) {
                Log.e("SMS", "处理短信失败", e)
            }
        }
    }
}
