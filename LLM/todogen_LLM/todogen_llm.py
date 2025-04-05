# todogen_llm.py
from openai import OpenAI
# from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from filter_useful_data_to_dict import get_formatted_data
from filter_message_list import get_message_ids
from config_loader import get_mysql_config, get_openai_config
from pathlib import Path
import argparse
import json
import sys
import os
import re
import io


# 修正后的标准流编码设置
sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', write_through=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', write_through=True)

client = OpenAI(
    api_key=get_openai_config()['api_key'],
    base_url=get_openai_config()['base_url']
)

# async def load_formatted_data():
def load_formatted_data():
    """同步获取格式化数据"""
    db_config = {
        "host": get_mysql_config()['host'],
        "database": get_mysql_config()['database'],
        "password": get_mysql_config()['password']
    }

    target_ids = get_message_ids()
    # return await get_formatted_data(db_config, target_ids)
    return get_formatted_data(db_config, target_ids)

def process_data(input_data: dict) -> dict:
    """处理字典数据并直接返回结果"""
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in input_data.items()):
        raise ValueError("输入数据为空")
    
    results = {}
    errors = {}

    # 将数据分批次处理（每10条为一批）
    items = list(input_data.items())
    batch_size = 8
    threads_per_batch = 3

    for batch_idx in range(0, len(items), batch_size):
        batch = items[batch_idx:batch_idx + batch_size]
        print(f"🚀 正在处理批次 {batch_idx//batch_size + 1}/{len(items)//batch_size + 1}")
        
        with ThreadPoolExecutor(max_workers=threads_per_batch) as executor:
            futures = {
                executor.submit(process_single_message, msg_id, content): msg_id
                for msg_id, content in batch
            }
            
            for future in as_completed(futures):
                msg_id = futures[future]
                try:
                    result = future.result()
                    clean_result = json_parser(result)
                    results.update(clean_result)
                except Exception as e:
                    error_msg = f"处理消息时出错: {str(e)}"
                    print(f"消息ID {msg_id}: {error_msg}")
                    errors[msg_id] = error_msg

    if errors:
        with open("processing_errors.json", "w", encoding="utf-8") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
        print(f"错误信息已保存到 processing_errors.json，共 {len(errors)} 条")

    return results

def process_single_message(msg_id: str, content: str) -> str:
    """单条消息处理包装函数"""
    try:
        return extract_single_message(msg_id, content)
    except Exception as e:
        raise RuntimeError(f"API调用失败: {str(e)}") from e

def json_parser(raw_text):
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        # 尝试从文本中提取JSON部分
        json_str = re.search(r'{.*}', raw_text, re.DOTALL)
        if json_str:
            try:
                return json.loads(json_str.group())
            except json.JSONDecodeError:
                # 清理引号问题
                cleaned_text = json_str.group().replace("'", '"')
                return json.loads(cleaned_text)
            
        else:
            return {"error": "无法从文本中提取JSON", "raw_text": raw_text}

def extract_single_message(message_id, content):
    """处理单条消息并提取关键信息"""
    # 转义特殊字符，防止内容破坏JSON格式
    content_escaped = content.replace('"', '\\"').replace('\n', '\\n')
    
    combined_prompt = f"""
# 任务说明
你是一个高级信息提取AI，需要从各类消息中智能识别并结构化以下关键信息：
1. 截止时间（end_time）
2. 地点信息（location）
3. 待办内容（todo_content）
4. 紧急程度（urgency）

# 核心能力要求
## 1. 时空理解能力
- 能识别绝对时间（如"2025-03-31 12:00"）
- 能计算相对时间（如"12小时内"需结合开始时间计算）
- 能区分线上/线下场景

## 2. 语义推理能力
- 对模糊表述进行合理推断（如"尽快取件"可视为urgent）
- 识别同义表述（如"丰巢柜/快递柜/智能柜"统一为丰巢快递柜）
- 处理不完整信息（如缺少具体时间时标记为null）

## 3. 场景适应能力
能处理以下典型场景：
✅ 物流快递（取件码/驿站通知）
✅ 外卖配送（智能柜存放）
✅ 会议邀约（线上/线下会议）
✅ 待办提醒（含时间要求的任务）
✅ 其他临时性事务

# 输入数据
{{
    "message_id": "{message_id}",
    "content": "{content_escaped}"
}}

# 处理规则
## 时间处理
1. 优先提取显式时间（如"23:00前"）
2. 次选相对时间（如"存柜超过12小时"需计算）
3. 无时间线索则填null
4. 如果开始时间start_time(开始时间为2025-03-31T15:01:37)或者 duration/时长 （如会议时长）都有，那end time = start time  + countdown 或者 end time = start time  + duration”

## 地点处理
1. 线上场景标记平台/工具（如"飞书会议"）
2. 线下场景提取完整地址
3. 模糊地址需补充特征（如"公司前台"）

## 内容提炼
1. 采用"动词+核心名词"结构
2. 保留业务关键词（如"PR Merge讨论"）
3. 去除修饰性词语

## 紧急程度
- urgent：需立即处理（如"即将超时"）
- important：有时限要求
- unimportant：无时间压力

# 输出示例
```json
{{
    "{message_id}": {{
        "end_time": "ISO8601格式或null",
        "location": "类型(线上/线下):具体描述",
        "todo_content": "最简任务描述",
        "urgency": "urgent/important/unimportant"
    }}
}}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": combined_prompt}],
        temperature=0.0
    )
    return response.choices[0].message.content

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='关键信息提取工具')
    default_output = str(Path(r"D:\python_study\ILoveDo\todogen_LLM\result.json"))
    parser.add_argument('-o', '--output', default=default_output, help='输出文件路径')
    
    try:
        args = parser.parse_args()
        # formatted_data = asyncio.run(load_formatted_data()) # 直接同步调用
        formatted_data = load_formatted_data() # 直接同步调用
        results = process_data(formatted_data)
        
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print("✅ 处理完成！结果已保存至:", args.output)
        
    except Exception as e:
        print(f"运行错误: {str(e)}")
        sys.exit(1)


"""
1、对硬编码密钥。数据库配置信息进行隐藏处理
2、2. LLM 提示模板关键问题
最关键的问题是 extract_single_message() 函数中的提示模板没有正确引用传入的 message_id 和 content 参数

"""