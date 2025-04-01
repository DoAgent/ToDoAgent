package com.asap.todoexmple.service

import android.util.Log

object NotificationPackages {
    private val notificationPackages = mutableSetOf<String>()
    private val observers = mutableListOf<(Set<String>) -> Unit>()

    fun addPackage(packageName: String) {
        Log.d("NotificationPackages", "添加包名: $packageName")
        if (notificationPackages.add(packageName)) {
            Log.d("NotificationPackages", "新包名已添加，当前包列表: ${notificationPackages.joinToString()}")
            notifyObservers()
        }
    }

    fun getAllPackages(): Set<String> {
        return notificationPackages.toSet()
    }

    // 添加观察者
    fun addObserver(observer: (Set<String>) -> Unit) {
        observers.add(observer)
    }

    // 移除观察者
    fun removeObserver(observer: (Set<String>) -> Unit) {
        observers.remove(observer)
    }

    // 通知所有观察者
    private fun notifyObservers() {
        val packages = getAllPackages()
        observers.forEach { it(packages) }
    }
} 