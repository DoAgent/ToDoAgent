package com.asap.todoexmple.fragment

import android.content.Context
import android.content.Intent
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.os.Build
import android.os.Bundle
import android.util.Log
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.Toast
import androidx.fragment.app.Fragment
import androidx.lifecycle.lifecycleScope
import com.asap.todoexmple.activity.MainActivity
import com.asap.todoexmple.databinding.FragmentRegisterBinding
import com.asap.todoexmple.util.DatabaseHelper
import com.asap.todoexmple.util.LocalDatabaseHelper
import com.asap.todoexmple.util.SessionManager
import com.asap.todoexmple.util.UserManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.regex.Pattern
import com.asap.todoexmple.util.PasswordUtils

class RegisterFragment : Fragment() {
    private var _binding: FragmentRegisterBinding? = null
    private val binding get() = _binding!!

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View {
        _binding = FragmentRegisterBinding.inflate(inflater, container, false)
        return binding.root
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)
        
        binding.btnRegister.setOnClickListener {
            validateAndRegister()
        }
    }

    private fun validateAndRegister() {
        val username = binding.etUsername.text.toString().trim()
        val email = binding.etEmail.text.toString().trim()
        val password = binding.etPassword.text.toString()
        val confirmPassword = binding.etConfirmPassword.text.toString()

        // 验证输入
        if (!validateInput(username, email, password, confirmPassword)) {
            return
        }

        // 显示加载状态
        setLoadingState(true)

        // 执行注册
        lifecycleScope.launch {
            try {
                // 确保初始化数据库
                context?.let { ctx ->
                    DatabaseHelper.initialize(ctx)
                    Log.d("RegisterFragment", "数据库已初始化")
                } ?: run {
                    showError("无法初始化应用")
                    return@launch
                }

                // 检查网络连接
                if (!isNetworkAvailable()) {
                    showError("请检查网络连接")
                    return@launch
                }

                // 检查用户名
                try {
                    if (!DatabaseHelper.checkUsernameAvailable(username)) {
                        showError("用户名已被使用")
                        return@launch
                    }
                } catch (e: Exception) {
                    Log.e("RegisterFragment", "检查用户名失败", e)
                    showError("检查用户名时出错: ${e.message ?: "未知错误"}")
                    return@launch
                }

                // 检查邮箱
                try {
                    if (!DatabaseHelper.checkEmailAvailable(email)) {
                        showError("邮箱已被注册")
                        return@launch
                    }
                } catch (e: Exception) {
                    Log.e("RegisterFragment", "检查邮箱失败", e)
                    showError("检查邮箱时出错: ${e.message ?: "未知错误"}")
                    return@launch
                }

                // 生成盐值和密码哈希
                val salt = try {
                    PasswordUtils.generateSalt()
                } catch (e: Exception) {
                    showError("生成安全密钥时出错：${e.message}")
                    return@launch
                }

                val hashedPassword = try {
                    PasswordUtils.hashPassword(password, salt)
                } catch (e: Exception) {
                    showError("处理密码时出错：${e.message}")
                    return@launch
                }

                // 注册用户
                try {
                    val userId = DatabaseHelper.registerUser(username, email, hashedPassword, salt)
                    if (userId != null) {
                        showSuccess("注册成功")
                        // 注册成功后自动登录
                        handleRegistrationSuccess(userId, username, email)
                        
                    } else {
                        showError("注册失败：用户创建失败")
                    }
                } catch (e: Exception) {
                    Log.e("RegisterFragment", "注册用户失败", e)
                    showError("注册过程中出错：${e.message ?: "未知错误"}")
                }
            } catch (e: Exception) {
                Log.e("RegisterFragment", "注册过程中发生错误", e)
                showError("注册失败: ${e.message ?: "未知错误"}")
            } finally {
                setLoadingState(false)
            }
        }
    }

    private suspend fun handleRegistrationSuccess(userId: String, username: String, email: String) {
        try {
            // 1. 保存会话信息
            withContext(Dispatchers.Main) {
                SessionManager.Auth.loginAfterRegister(requireContext(), userId, username)
            }

            // 2. 保存到本地数据库
            withContext(Dispatchers.IO) {
                LocalDatabaseHelper.saveUserInfo(requireContext(), userId, username)
                LocalDatabaseHelper.setupPeriodicSync(requireContext(), userId)
            }

            // 3. 保存到UserManager
            withContext(Dispatchers.Main) {
                UserManager.saveUserInfo(requireContext(), userId, username, email)
            }

            // 4. 启动MainActivity
            withContext(Dispatchers.Main) {
                val intent = Intent(requireContext(), MainActivity::class.java).apply {
                    flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK
                }
                startActivity(intent)
                activity?.finish()
            }

            // 5. 显示成功消息
            withContext(Dispatchers.Main) {
                Toast.makeText(requireContext(), "注册成功并已登录", Toast.LENGTH_SHORT).show()
            }
        } catch (e: Exception) {
            Log.e("RegisterFragment", "注册后登录失败", e)
            throw Exception("注册后初始化失败：${e.message}")
        }
    }

    private fun validateInput(
        username: String,
        email: String,
        password: String,
        confirmPassword: String
    ): Boolean {
        // 验证用户名
        if (username.length < 3 || username.length > 20) {
            showError("用户名长度应在3-20个字符之间")
            return false
        }

        // 验证邮箱格式
        if (!isValidEmail(email)) {
            showError("请输入有效的邮箱地址")
            return false
        }

        // 验证密码
        if (password.length < 6) {
            showError("密码长度不能少于6个字符")
            return false
        }

        // 验证确认密码
        if (password != confirmPassword) {
            showError("两次输入的密码不一致")
            return false
        }

        return true
    }

    private fun isValidEmail(email: String): Boolean {
        val emailPattern = Pattern.compile(
            "[a-zA-Z0-9+._%\\-]{1,256}" +
                    "@" +
                    "[a-zA-Z0-9][a-zA-Z0-9\\-]{0,64}" +
                    "(" +
                    "\\." +
                    "[a-zA-Z0-9][a-zA-Z0-9\\-]{0,25}" +
                    ")+"
        )
        return emailPattern.matcher(email).matches()
    }

    private fun setLoadingState(isLoading: Boolean) {
        binding.apply {
            btnRegister.isEnabled = !isLoading
            etUsername.isEnabled = !isLoading
            etEmail.isEnabled = !isLoading
            etPassword.isEnabled = !isLoading
            etConfirmPassword.isEnabled = !isLoading
            
            // 添加进度条显示
            progressBar.visibility = if (isLoading) View.VISIBLE else View.GONE
        }
    }

    private fun showError(message: String) {
        Toast.makeText(context, message, Toast.LENGTH_SHORT).show()
    }

    private fun showSuccess(message: String) {
        Toast.makeText(context, message, Toast.LENGTH_SHORT).show()
    }

    private fun isNetworkAvailable(): Boolean {
        val connectivityManager = context?.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
        if (connectivityManager != null) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                val network = connectivityManager.activeNetwork
                val capabilities = connectivityManager.getNetworkCapabilities(network)
                return capabilities != null && capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
            } else {
                @Suppress("DEPRECATION")
                val networkInfo = connectivityManager.activeNetworkInfo
                return networkInfo != null && networkInfo.isConnected
            }
        }
        return false
    }

    override fun onDestroyView() {
        super.onDestroyView()
        _binding = null
    }
} 