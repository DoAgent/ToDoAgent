package com.asap.todoexmple.adapter

import android.view.LayoutInflater
import android.view.ViewGroup
import androidx.recyclerview.widget.DiffUtil
import androidx.recyclerview.widget.ListAdapter
import androidx.recyclerview.widget.RecyclerView
import com.asap.todoexmple.databinding.ItemLoginHistoryBinding
import com.asap.todoexmple.model.LoginHistoryItem

class LoginHistoryAdapter(
    private val onItemClick: (LoginHistoryItem) -> Unit
) : ListAdapter<LoginHistoryItem, LoginHistoryAdapter.ViewHolder>(LoginHistoryDiffCallback()) {

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): ViewHolder {
        val binding = ItemLoginHistoryBinding.inflate(
            LayoutInflater.from(parent.context),
            parent,
            false
        )
        return ViewHolder(binding)
    }

    override fun onBindViewHolder(holder: ViewHolder, position: Int) {
        holder.bind(getItem(position))
    }

    inner class ViewHolder(
        private val binding: ItemLoginHistoryBinding
    ) : RecyclerView.ViewHolder(binding.root) {
        
        init {
            binding.root.setOnClickListener {
                val position = bindingAdapterPosition
                if (position != RecyclerView.NO_POSITION) {
                    onItemClick(getItem(position))
                }
            }
        }

        fun bind(item: LoginHistoryItem) {
            binding.apply {
                tvUsername.text = item.username
                tvLastLoginTime.text = item.lastLoginTime
                // 如果有需要，这里可以使用Glide或Coil加载头像
                // Glide.with(ivAvatar).load(item.avatarUrl).into(ivAvatar)
            }
        }
    }

    private class LoginHistoryDiffCallback : DiffUtil.ItemCallback<LoginHistoryItem>() {
        override fun areItemsTheSame(oldItem: LoginHistoryItem, newItem: LoginHistoryItem): Boolean {
            return oldItem.username == newItem.username
        }

        override fun areContentsTheSame(oldItem: LoginHistoryItem, newItem: LoginHistoryItem): Boolean {
            return oldItem == newItem
        }
    }
} 