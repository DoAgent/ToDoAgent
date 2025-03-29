package com.asap.todoexmple.fragment

import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import androidx.fragment.app.Fragment
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.asap.todoexmple.R

class CalendarFragment : Fragment() {
    private lateinit var taskList: RecyclerView

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View? {
        return inflater.inflate(R.layout.fragment_calendar, container, false)
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)
        initViews(view)
        setupCalendarView()
    }

    private fun initViews(view: View) {
        taskList = view.findViewById(R.id.taskList)
    }

    private fun setupCalendarView() {
        // TODO: 初始化日历视图
        taskList.layoutManager = LinearLayoutManager(context)
        // TODO: 设置日历任务列表适配器
    }
} 