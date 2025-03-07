package com.asap.todoexmple.receiver;

import android.annotation.SuppressLint;
import android.content.BroadcastReceiver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.provider.Telephony;
import android.telephony.SmsMessage;
import android.os.Bundle;
import android.util.Log;

import java.util.Objects;

public class SmsReceiver extends BroadcastReceiver {
    public static final String SMS_RECEIVED_ACTION = "com.asap.complexest.SMS_RECEIVED_ACTION";
    @SuppressLint("UnsafeProtectedBroadcastReceiver")
    @Override
    public void onReceive(Context context, Intent intent) {
        if (!Objects.equals(intent.getAction(), "android.provider.Telephony.SMS_RECEIVED")) {
            return;
        }
        Bundle bundle = intent.getExtras();
        if (bundle != null) {
            Object[] pdus = (Object[]) bundle.get("pdus"); // 获取短信数据
            if (pdus != null) {
                SmsMessage[] messages = new SmsMessage[pdus.length];
                String number = "";
                StringBuilder fullMessage = new StringBuilder();
                long date = 0;
                for (int i = 0; i < pdus.length; i++) {
                    messages[i] = SmsMessage.createFromPdu((byte[]) pdus[i]);
                    if (i == 0) {
                        number = messages[i].getDisplayOriginatingAddress();
                        date = messages[i].getTimestampMillis();
                    }
                    fullMessage.append(messages[i].getDisplayMessageBody());
                }
                processSms(context, number, fullMessage.toString(), date, Telephony.TextBasedSmsColumns.MESSAGE_TYPE_INBOX);
            }
        }
    }

    private void processSms(Context context, String sender, String messageBody, long timestamp, int messageType) {
        // 实现短信处理逻辑（例如保存到数据库或显示通知）
        Log.d("SMS", "来自：" + sender + "，内容：" + messageBody);

        // 示例：保存到数据库（需自行实现数据库操作）
        ContentValues values = new ContentValues();
        values.put("address", sender);
        values.put("body", messageBody);
        values.put("date", timestamp);
        values.put("type", messageType);

        // 发送广播到MainActivity
        Intent broadcastIntent = new Intent(SMS_RECEIVED_ACTION);
        broadcastIntent.putExtra("sender", sender);
        broadcastIntent.putExtra("body", messageBody);
        broadcastIntent.putExtra("timestamp", timestamp);
        context.sendBroadcast(broadcastIntent);
    }
}
