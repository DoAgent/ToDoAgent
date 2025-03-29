package com.asap.todoexmple.activity

import android.os.Bundle
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.fragment.app.Fragment
import androidx.viewpager2.adapter.FragmentStateAdapter

import com.asap.todoexmple.databinding.ActivityLoginBinding
import com.asap.todoexmple.fragment.LoginFragment
import com.asap.todoexmple.fragment.RegisterFragment
import com.google.android.material.tabs.TabLayoutMediator

class LoginActivity : AppCompatActivity() {
    private lateinit var binding: ActivityLoginBinding
    
    // 添加返回键相关变量
    private var lastBackPressTime: Long = 0
    private val BACK_PRESS_INTERVAL = 2000 // 两次返回键间隔时间(毫秒)
    
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityLoginBinding.inflate(layoutInflater)
        setContentView(binding.root)

        setupToolbar()
        setupViewPager()
    }

    private fun setupToolbar() {
        binding.toolbar.setNavigationOnClickListener {
            // 使用与返回键相同的退出逻辑
            handleBackPress()
        }
    }

    private fun setupViewPager() {
        binding.viewPager.adapter = object : FragmentStateAdapter(this) {
            override fun getItemCount() = 2
            override fun createFragment(position: Int): Fragment {
                return when (position) {
                    0 -> LoginFragment()
                    else -> RegisterFragment()
                }
            }
        }

        TabLayoutMediator(binding.tabLayout, binding.viewPager) { tab, position ->
            tab.text = when (position) {
                0 -> "登录"
                else -> "注册"
            }
        }.attach()
    }

    // 抽取共用的退出逻辑
    private fun handleBackPress() {
        val currentTime = System.currentTimeMillis()
        if (currentTime - lastBackPressTime > BACK_PRESS_INTERVAL) {
            Toast.makeText(this, "再按一次退出应用", Toast.LENGTH_SHORT).show()
            lastBackPressTime = currentTime
        } else {
            finish()
        }
    }

    override fun onBackPressed() {
        handleBackPress()
    }
}