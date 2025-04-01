package com.asap.todoexmple.util

import java.security.MessageDigest
import java.security.SecureRandom
import android.util.Base64

object PasswordUtils {
    private const val SALT_LENGTH = 16 // 盐值长度

    // 生成随机盐值
    fun generateSalt(): String {
        val salt = ByteArray(SALT_LENGTH)
        SecureRandom().nextBytes(salt)
        return Base64.encodeToString(salt, Base64.NO_WRAP)
    }

    // 使用SHA-256进行密码哈希
    fun hashPassword(password: String, salt: String): String {
        val messageDigest = MessageDigest.getInstance("SHA-256")
        val saltBytes = Base64.decode(salt, Base64.NO_WRAP)
        
        // 将密码和盐值组合
        val input = password.toByteArray() + saltBytes
        
        // 计算哈希值
        val hashBytes = messageDigest.digest(input)
        
        // 将哈希值转换为Base64字符串
        return Base64.encodeToString(hashBytes, Base64.NO_WRAP)
    }

    // 验证密码
    fun verifyPassword(password: String, salt: String, hashedPassword: String): Boolean {
        val computedHash = hashPassword(password, salt)
        return computedHash == hashedPassword
    }
} 