package com.asap.todoexmple.util

import android.content.Context
import android.content.SharedPreferences

object UserManager {
    private const val PREF_NAME = "user_prefs"
    private const val KEY_IS_LOGGED_IN = "is_logged_in"
    private const val KEY_USER_NAME = "user_name"
    private const val KEY_USER_ID = "user_id"
    private const val KEY_USER_EMAIL = "user_email"

    private fun getPrefs(context: Context): SharedPreferences {
        return context.getSharedPreferences(PREF_NAME, Context.MODE_PRIVATE)
    }

    fun saveUserInfo(context: Context, userId: String, userName: String, email: String) {
        getPrefs(context).edit().apply {
            putBoolean(KEY_IS_LOGGED_IN, true)
            putString(KEY_USER_ID, userId)
            putString(KEY_USER_NAME, userName)
            putString(KEY_USER_EMAIL, email)
        }.apply()
    }

    fun clearUserInfo(context: Context) {
        getPrefs(context).edit().clear().apply()
    }

    fun isLoggedIn(context: Context): Boolean {
        return getPrefs(context).getBoolean(KEY_IS_LOGGED_IN, false)
    }

    fun getUserName(context: Context): String {
        return getPrefs(context).getString(KEY_USER_NAME, "") ?: ""
    }

    fun getUserId(context: Context): String {
        return getPrefs(context).getString(KEY_USER_ID, "") ?: ""
    }

    fun getUserEmail(context: Context): String {
        return getPrefs(context).getString(KEY_USER_EMAIL, "") ?: ""
    }
} 