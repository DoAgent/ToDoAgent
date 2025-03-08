package com.asap.todoexmple.application

import android.app.Application

class YourApplication : Application() {
    lateinit var smsViewModel: SmsViewModel

    override fun onCreate() {
        super.onCreate()
        smsViewModel = SmsViewModel()
    }
}