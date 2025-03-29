package com.asap.todoagent.ui.important

import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.TextView
import androidx.fragment.app.Fragment
import androidx.lifecycle.ViewModelProvider
import com.asap.todoagent.databinding.FragmentImportantBinding

class ImportantFragment : Fragment() {

    private var _binding: FragmentImportantBinding? = null

    // This property is only valid between onCreateView and
    // onDestroyView.
    private val binding get() = _binding!!

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View {
        val slideshowViewModel =
            ViewModelProvider(this).get(ImportantViewModel::class.java)

        _binding = FragmentImportantBinding.inflate(inflater, container, false)
        val root: View = binding.root

        val textView: TextView = binding.textImportant
        slideshowViewModel.text.observe(viewLifecycleOwner) {
            textView.text = it
        }
        return root
    }

    override fun onDestroyView() {
        super.onDestroyView()
        _binding = null
    }
}