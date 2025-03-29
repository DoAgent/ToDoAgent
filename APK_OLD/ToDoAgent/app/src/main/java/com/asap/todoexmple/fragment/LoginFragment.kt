package com.asap.todoexmple.fragment

import android.content.Intent
import android.os.Bundle
import android.util.Log
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.Toast
import androidx.fragment.app.Fragment
import androidx.lifecycle.lifecycleScope
import com.asap.todoexmple.activity.MainActivity
import com.asap.todoexmple.databinding.FragmentLoginBinding
import com.asap.todoexmple.util.DatabaseHelper
import com.asap.todoexmple.util.LocalDatabaseHelper
import com.asap.todoexmple.util.SessionManager
import com.asap.todoexmple.util.UserManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.security.MessageDigest

class LoginFragment : Fragment() {
    private var _binding: FragmentLoginBinding? = null
    private val binding get() = _binding!!

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View {
        _binding = FragmentLoginBinding.inflate(inflater, container, false)
        return binding.root
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)

        // 如果已经登录，直接跳转到主页
        if (SessionManager.Session.isLoggedIn(requireContext())) {
            startMainActivity()
            return
        }

        binding.btnLogin.setOnClickListener {
            val account = binding.etAccount.text.toString().trim()
            val password = binding.etPassword.text.toString()

            if (account.isEmpty() || password.isEmpty()) {
                Toast.makeText(context, "账号和密码不能为空", Toast.LENGTH_SHORT).show()
                return@setOnClickListener
            }

            // 显示加载状态
            setLoadingState(true)

            // 执行登录
            lifecycleScope.launch(Dispatchers.Main) {
                try {
                    val loginResult = withContext(Dispatchers.IO) {
                        performLogin(account, password)
                    }

                    if (loginResult != null) {
                        val (userId, username) = loginResult
                        handleLoginSuccess(userId, username)
                    } else {
                        Toast.makeText(context, "账号或密码错误", Toast.LENGTH_SHORT).show()
                    }
                } catch (e: Exception) {
                    Log.e(TAG, "登录失败", e)
                    Toast.makeText(context, "登录失败：${e.message}", Toast.LENGTH_SHORT).show()
                } finally {
                    setLoadingState(false)
                }
            }
        }
    }

    private suspend fun performLogin(account: String, password: String): Pair<String, String>? {
        return withContext(Dispatchers.IO) {
            try {
                val connection = DatabaseHelper.getConnection() ?: throw Exception("无法连接到服务器")

                try {
                    val query = """
                        SELECT user_id, user_name, pwd_hash, pwd_salt 
                        FROM Users 
                        WHERE user_id = ? 
                        OR user_name = ? 
                        OR user_mail = ?
                    """

                    Log.d(TAG, "尝试登录: $account")

                    connection.prepareStatement(query).use { stmt ->
                        stmt.setString(1, account)
                        stmt.setString(2, account)
                        stmt.setString(3, account)

                        stmt.executeQuery().use { rs ->
                            if (rs.next()) {
                                val userId = rs.getString("user_id")
                                val username = rs.getString("user_name")
                                val storedHash = rs.getString("pwd_hash")
                                val salt = rs.getString("pwd_salt")

                                val hashedPassword = hashPassword(password, salt)
                                if (hashedPassword == storedHash) {
                                    Log.d(TAG, "登录成功: $username")
                                    Pair(userId, username)
                                } else {
                                    null
                                }
                            } else {
                                null
                            }
                        }
                    }
                } finally {
                    DatabaseHelper.releaseConnection(connection)
                }
            } catch (e: Exception) {
                Log.e(TAG, "登录验证失败", e)
                throw Exception("登录验证失败：${e.message}")
            }
        }
    }

    private suspend fun handleLoginSuccess(userId: String, username: String) {
        try {
            // 1. 保存会话信息
            SessionManager.Auth.login(requireContext(), userId, username)

            // 2. 保存到本地数据库
            withContext(Dispatchers.IO) {
                LocalDatabaseHelper.saveUserInfo(requireContext(), userId, username)
                LocalDatabaseHelper.setupPeriodicSync(requireContext(), userId)
            }

            // 3. 保存到UserManager
            UserManager.saveUserInfo(requireContext(), userId, username, null.toString())

            // 4. 启动MainActivity
            startMainActivity()

            // 5. 显示成功消息
            Toast.makeText(requireContext(), "登录成功", Toast.LENGTH_SHORT).show()
        } catch (e: Exception) {
            Log.e(TAG, "登录后处理失败", e)
            throw Exception("登录后初始化失败：${e.message}")
        }
    }

    private fun hashPassword(password: String, salt: String): String {
        try {
            val md = MessageDigest.getInstance("SHA-256")
            val decodedSalt = android.util.Base64.decode(salt, android.util.Base64.DEFAULT)
            val input = password.toByteArray(Charsets.UTF_8) + decodedSalt
            val hashBytes = md.digest(input)
            return android.util.Base64.encodeToString(hashBytes, android.util.Base64.DEFAULT).trim()
        } catch (e: Exception) {
            Log.e(TAG, "密码哈希计算失败", e)
            throw Exception("密码处理失败")
        }
    }

    private fun setLoadingState(isLoading: Boolean) {
        binding.btnLogin.isEnabled = !isLoading
        binding.btnLogin.text = if (isLoading) "登录中..." else "登录"
        binding.etAccount.isEnabled = !isLoading
        binding.etPassword.isEnabled = !isLoading
    }

    private fun startMainActivity() {
        val intent = Intent(requireContext(), MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK
        }
        startActivity(intent)
        activity?.finish()
    }

    override fun onDestroyView() {
        super.onDestroyView()
        _binding = null
    }

    companion object {
        private const val TAG = "LoginFragment"
    }
} 