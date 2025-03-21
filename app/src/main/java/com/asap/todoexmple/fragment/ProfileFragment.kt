package com.asap.todoexmple.fragment

import android.content.Intent
import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.TextView
import androidx.fragment.app.Fragment
import com.asap.todoexmple.R
import com.asap.todoexmple.activity.SettingsActivity
import com.asap.todoexmple.util.SessionManager
import com.asap.todoexmple.activity.LoginActivity

class ProfileFragment : Fragment() {
    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View? {
        return inflater.inflate(R.layout.fragment_profile, container, false)
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)
        
        // 获取用户名显示控件
        val userNameTextView = view.findViewById<TextView>(R.id.userName)
        
        // 更新用户名显示
        updateUserNameDisplay(userNameTextView)

        // 设置按钮点击事件
        view.findViewById<View>(R.id.settingsFragment).setOnClickListener {
            // 启动设置Activity
            startActivity(Intent(requireContext(), SettingsActivity::class.java))
        }

        // 如果显示的是"登录/注册"，添加点击事件
        userNameTextView.setOnClickListener {
            if (!SessionManager.Session.isLoggedIn(requireContext())) {
                startActivity(Intent(requireContext(), LoginActivity::class.java))
            }
        }
    }

    override fun onResume() {
        super.onResume()
        // 在页面恢复时更新用户名显示，以处理用户登录/登出后的状态变化
        view?.findViewById<TextView>(R.id.userName)?.let { 
            updateUserNameDisplay(it) 
        }
    }

    private fun updateUserNameDisplay(userNameTextView: TextView) {
        val context = requireContext()
        if (SessionManager.Session.isLoggedIn(context)) {
            // 已登录，显示用户名
            val username = SessionManager.Session.getUsername(context)
            userNameTextView.text = username ?: "未知用户"
        } else {
            // 未登录，显示"登录/注册"
            userNameTextView.text = "登录/注册"
        }
    }
} 