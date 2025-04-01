package com.asap.todoexmple.model

data class LoginHistoryItem(
    val username: String,
    val lastLoginTime: String,
    val avatarUrl: String? = null
) 