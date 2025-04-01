package com.asap.todoexmple.adapter

import android.content.Context
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ImageView
import android.widget.TextView
import androidx.appcompat.widget.SwitchCompat
import androidx.recyclerview.widget.RecyclerView
import com.asap.todoexmple.R
import com.asap.todoexmple.model.AppInfo

class AppListAdapter(private val apps: List<AppInfo>) : 
    RecyclerView.Adapter<AppListAdapter.AppViewHolder>() {

    companion object {
        private const val PREF_NAME = "notification_settings"
    }

    class AppViewHolder(view: View) : RecyclerView.ViewHolder(view) {
        val appIcon: ImageView = view.findViewById(R.id.appIcon)
        val appName: TextView = view.findViewById(R.id.appName)
        val toggleSwitch: SwitchCompat = view.findViewById(R.id.toggleSwitch)
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): AppViewHolder {
        val view = LayoutInflater.from(parent.context)
            .inflate(R.layout.item_app, parent, false)
        return AppViewHolder(view)
    }

    override fun onBindViewHolder(holder: AppViewHolder, position: Int) {
        val app = apps[position]
        holder.appIcon.setImageDrawable(app.appIcon)
        holder.appName.text = app.appName
        
        // 从 SharedPreferences 读取开关状态
        val sharedPrefs = holder.itemView.context.getSharedPreferences(PREF_NAME, Context.MODE_PRIVATE)
        holder.toggleSwitch.isChecked = sharedPrefs.getBoolean(app.packageName, false)
        
        // 保存开关状态到 SharedPreferences
        holder.toggleSwitch.setOnCheckedChangeListener { _, isChecked ->
            sharedPrefs.edit().putBoolean(app.packageName, isChecked).apply()
        }
    }

    override fun getItemCount() = apps.size
} 