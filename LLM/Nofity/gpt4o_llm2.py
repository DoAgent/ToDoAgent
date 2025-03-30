
from openai import OpenAI
import json
import argparse
import os
import sys
import re
from pathlib import Path


# 强制设置编码环境
os.environ['PYTHONIOENCODING'] = 'utf-8-sig'

client = OpenAI(
    api_key="sk-o4NKlYiGRlESc8UK669cAdEd5f5a4b428bD21f9334680bFa",
    base_url="https://aihubmix.com/v1"
)

def process_json_file(input_file, output_file):
    """处理JSON文件的核心工作流"""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            input_data = json.load(f)
    except Exception as e:
        print(f"文件读取失败: {str(e)}")
        sys.exit(1)

    results = {}
    for message_id, content in input_data.items():
        try:
            result = extract_single_message(message_id, content)
            clean_result = json_parser(result)  # 新增JSON清洗步骤
            results.update(clean_result)
        except Exception as e:
            print(f"处理消息 {message_id} 时出错: {str(e)}")
    
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

def json_parser(raw_text):
    """增强型JSON解析器（处理非标准响应）"""
    try:
        # 尝试直接解析
        return json.loads(raw_text)
    except json.JSONDecodeError:
        # 使用正则表达式提取可能的JSON部分
        json_str = re.search(r'\{.*\}', raw_text, re.DOTALL)
        if json_str:
            try:
                return json.loads(json_str.group())
            except:
                pass
        # 最终兜底方案
        return {"error": "无法解析的响应"}

def extract_single_message(message_id, content):
    """处理单个消息的原子操作（适配定制模型）"""
    combined_prompt = f"""请严格按照以下JSON格式输出：
{{
    "{message_id}": {{
        "end_time": "时间或null",
        "location": "类型:具体地点",
        "todo_content": "简明描述"
    }}
}}

需要分析的消息内容：
{content}

注意：
1. 时间格式必须是YYYY-MM-DD HH:mm:ss
2. 地点必须明确标注线上/线下
3. 不要添加任何额外说明"""

    try:
        response = client.chat.completions.create(
            model="o1-mini",
            messages=[{"role": "user", "content": combined_prompt}],
            temperature=0.1
        )
        raw_output = response.choices[0].message.content
        
        # 记录原始响应用于调试
        print(f"原始响应：{raw_output}")
        
        return raw_output
    except Exception as e:
        print(f"API请求失败: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='关键信息提取工具',
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    default_input = str(Path(r'D:\python_study\ILoveDo\useful_message.json'))
    default_output = str(Path(r"D:\python_study\ILoveDo\result.json"))

    parser.add_argument('-i', '--input', default=default_input, 
                      help='输入文件路径（默认：D:\\python_study\\ILoveDo\\useful_message.json）')
    parser.add_argument('-o', '--output', default=default_output,
                      help='输出文件路径（默认：D:\\python_study\\ILoveDo\\result.json）')

    try:
        args = parser.parse_args()
        
        if not os.path.exists(args.input):
            print(f"错误：输入文件 {args.input} 不存在")
            sys.exit(1)
            
        process_json_file(args.input, args.output)
        print("✅ 处理完成！结果已保存至:", args.output)
        
    except Exception as e:
        print(f"运行错误: {str(e)}")
        sys.exit(1)