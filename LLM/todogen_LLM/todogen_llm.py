# todogen_llm.py
from openai import OpenAI
import json
import argparse
import os
import re
import asyncio
from pathlib import Path
from filter_useful_data_to_dict import get_formatted_data

import io
import sys

# 修正后的标准流编码设置
sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', write_through=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', write_through=True)

client = OpenAI(
    api_key="sk-o4NKlYiGRlESc8UK669cAdEd5f5a4b428bD21f9334680bFa",
    base_url="https://aihubmix.com/v1"
)

async def load_formatted_data():
    """异步获取格式化数据"""
    db_config = {
        "host": "103.116.245.150",
        "database": "ToDoAgent",
        "password": "4bc6bc963e6d8443453676"
    }
    target_ids = [
        327163713, 325202761, 325202741, 325151109,
        325151100, 325145820, 325144014, 324204487, 322085363
    ]
    return await get_formatted_data(db_config, target_ids)

def process_data(input_data: dict) -> dict:
    """处理字典数据并直接返回结果"""
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in input_data.items()):
        raise ValueError("输入数据格式异常")
    
    results = {}
    for message_id, content in input_data.items():
        try:
            result = extract_single_message(message_id, content)
            clean_result = json_parser(result)
            results.update(clean_result)
        except Exception as e:
            print(f"处理消息 {message_id} 时出错: {str(e)}")
    return results

def json_parser(raw_text):
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        json_str = re.search(r'\{.*\}', raw_text, re.DOTALL)
        return json.loads(json_str.group()) if json_str else {"error": "无法解析的响应"}

def extract_single_message(message_id, content):
    combined_prompt = f"""
请严格按照以下JSON格式输出：
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
3. 不要添加任何额外说明
"""

    response = client.chat.completions.create(
        model="o1-mini",
        messages=[{"role": "user", "content": combined_prompt}],
        temperature=0.1
    )
    return response.choices[0].message.content

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='关键信息提取工具')
    default_output = str(Path(r"D:\python_study\ILoveDo\data_use\result.json"))
    parser.add_argument('-o', '--output', default=default_output, help='输出文件路径')
    
    try:
        args = parser.parse_args()
        formatted_data = asyncio.run(load_formatted_data())
        results = process_data(formatted_data)
        
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print("✅ 处理完成！结果已保存至:", args.output)
        
    except Exception as e:
        print(f"运行错误: {str(e)}")
        sys.exit(1)