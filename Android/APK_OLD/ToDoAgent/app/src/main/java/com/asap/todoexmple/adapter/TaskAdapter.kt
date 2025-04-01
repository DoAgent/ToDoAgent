package com.asap.todoexmple.adapter

import Task
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.TextView
import android.widget.CheckBox
import android.widget.ImageButton
import androidx.recyclerview.widget.RecyclerView
import com.asap.todoexmple.R

class TaskAdapter(
    private val onImportantClick: (Task, Boolean) -> Unit,
    private val onCompletedClick: (Task, Boolean) -> Unit,
    private val onCalendarClick: (Task) -> Unit
) : RecyclerView.Adapter<TaskAdapter.TaskViewHolder>() {
    private var tasks: List<Task> = emptyList()

    inner class TaskViewHolder(view: View) : RecyclerView.ViewHolder(view) {
        val contentText: TextView = view.findViewById(R.id.tvContent)
        val timeText: TextView = view.findViewById(R.id.tvTime)
        val locationText: TextView = view.findViewById(R.id.tvLocation)
        val checkComplete: CheckBox = view.findViewById(R.id.checkComplete)
        val btnImportant: ImageButton = view.findViewById(R.id.btnImportant)
        private val btnCalendar: ImageButton = view.findViewById(R.id.btnCalendar)

        fun bind(task: Task) {
            // 设置基本信息
            contentText.text = task.content
            timeText.text = "${task.startTime ?: ""} - ${task.endTime ?: ""}"
            locationText.apply {
                text = task.location ?: ""
                visibility = if (task.location.isNullOrEmpty()) View.GONE else View.VISIBLE
            }
            
            // 设置完成状态
            checkComplete.apply {
                setOnCheckedChangeListener(null) // 清除之前的监听器
                isChecked = task.isCompleted
                setOnCheckedChangeListener { _, isChecked ->
                    task.isCompleted = isChecked // 更新本地状态
                    onCompletedClick(task, isChecked)
                }
            }
            
            // 设置重要状态
            btnImportant.apply {
                isSelected = task.isImportant
                setOnClickListener {
                    val newState = !isSelected
                    isSelected = newState
                    task.isImportant = newState // 更新本地状态
                    onImportantClick(task, newState)
                }
                
                // 设置重要按钮的图标状态
                setImageResource(
                    if (task.isImportant) R.drawable.ic_star_filled 
                    else R.drawable.ic_star_outline
                )
            }
            
            // 根据完成状态设置内容文本样式
            contentText.apply {
                alpha = if (task.isCompleted) 0.5f else 1.0f
                paintFlags = if (task.isCompleted) {
                    paintFlags or android.graphics.Paint.STRIKE_THRU_TEXT_FLAG
                } else {
                    paintFlags and android.graphics.Paint.STRIKE_THRU_TEXT_FLAG.inv()
                }
            }

            // 设置日历按钮点击事件
            btnCalendar.setOnClickListener {
                onCalendarClick(task)
            }
        }
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): TaskViewHolder {
        val view = LayoutInflater.from(parent.context)
            .inflate(R.layout.item_task, parent, false)
        return TaskViewHolder(view)
    }

    override fun onBindViewHolder(holder: TaskViewHolder, position: Int) {
        val task = tasks[position]
        holder.bind(task)
    }

    override fun getItemCount() = tasks.size

    fun updateTasks(newTasks: List<Task>) {
        tasks = newTasks
        notifyDataSetChanged()
    }
} 