import json
import csv
import os
import pandas as pd
import openai
import time
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# 加载环境变量（如果有.env文件）
load_dotenv()

# 配置SiliconFlow API
SILICONFLOW_API_KEY = os.getenv("SILICONFLOW_API_KEY", "sk-ypjvmantsostdxrkirhidrtswohjpmlzuhyqojpudbreakwk")
SILICONFLOW_API_BASE = os.getenv("SILICONFLOW_API_BASE", "https://api.siliconflow.cn/v1")

# 保留OpenAI API配置（作为备选）
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "your_openai_api_key_here")
openai.api_key = OPENAI_API_KEY

# 可以配置为Azure OpenAI
#AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")
#AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
#AZURE_DEPLOYMENT_NAME = os.getenv("AZURE_DEPLOYMENT_NAME", "")

# 获取Azure配置参数
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_DEPLOYMENT_NAME = os.getenv("AZURE_DEPLOYMENT_NAME", "")

# 如果有Azure OpenAI配置，则使用Azure OpenAI
if AZURE_OPENAI_ENDPOINT.strip() and AZURE_OPENAI_API_KEY.strip() and AZURE_DEPLOYMENT_NAME.strip():
    openai.api_type = "azure"
    openai.api_base = AZURE_OPENAI_ENDPOINT
    openai.api_key = AZURE_OPENAI_API_KEY
    openai.api_version = "2023-05-15"  # 可能需要根据实际情况调整

# 定义TruePositive的标准（根据mvp三类案例）
def define_positive_sample_criteria():
    """
    定义TruePositive的标准
    根据搜索结果，TruePositive被定义为"mvp三类案例"，但没有找到具体定义
    这里我们定义一些可能的标准，实际使用时可以根据需求调整
    """
    return """
    请判断以下消息是否属于TruePositive。TruePositive定义为与任务管理、待办事项、提醒、通知筛选相关的有用信息，具体包括：
    1. 包含明确的任务、待办事项或需要完成的工作
    2. 包含时间安排、截止日期或日程提醒
    3. 包含项目进展、状态更新或工作报告
    
    如果消息符合以上任一条件，则为TruePositive；否则为TrueNegative。
    请只回答"TruePositive"或"TrueNegative"。
    """

# 使用大模型API进行分类
def classify_with_llm(message, criteria, max_retries=3, retry_delay=2):
    """
    使用大模型API对消息进行分类
    
    Args:
        message: 要分类的消息内容
        criteria: 分类标准
        max_retries: 最大重试次数
        retry_delay: 重试延迟（秒）
        
    Returns:
        str: "TruePositive" 或 "TrueNegative"
    """
    prompt = f"{criteria}\n\n消息内容: {message}"
    system_message = "你是一个专业的数据分类助手，根据给定标准判断消息是TruePositive还是TrueNegative。"
    
    for attempt in range(max_retries):
        try:
            # 使用SiliconFlow API
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {SILICONFLOW_API_KEY}"
            }
            
            payload = {
                "model": "deepseek-ai/DeepSeek-V3",
                "messages": [
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": prompt}
                ],
                "stream": False,
                "max_tokens": 512,
                "temperature": 0.1,
                "top_p": 0.7,
                "top_k": 50,
                "frequency_penalty": 0.5,
                "n": 1
            }
            
            response = requests.post(
                f"{SILICONFLOW_API_BASE}/chat/completions",
                headers=headers,
                json=payload
            )
            
            # 检查响应状态
            response.raise_for_status()
            response_data = response.json()
            
            # 解析响应
            result = response_data["choices"][0]["message"]["content"].strip()
            
            # 标准化结果
            if "TruePositive" in result:
                return "TruePositive"
            else:
                return "TrueNegative"
                
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"API调用失败，{retry_delay}秒后重试: {e}\n响应状态码: {response.status_code if 'response' in locals() else 'N/A'}\n响应内容: {response.text if 'response' in locals() else 'N/A'}")
                time.sleep(retry_delay)
            else:
                print(f"API调用失败，达到最大重试次数: {e}\n最后响应状态码: {response.status_code if 'response' in locals() else 'N/A'}\n最后响应内容: {response.text if 'response' in locals() else 'N/A'}")
                return "分类失败"  # 返回一个默认值

# 批量处理消息
def batch_process_messages(messages, batch_size=10, delay=1):
    """
    批量处理消息以避免API限制
    
    Args:
        messages: 消息列表
        batch_size: 每批处理的消息数量
        delay: 批次间延迟（秒）
        
    Returns:
        list: 处理结果列表
    """
    results = []
    criteria = define_positive_sample_criteria()
    
    for i in tqdm(range(0, len(messages), batch_size), desc="处理批次"):
        batch = messages[i:i+batch_size]
        batch_results = []
        
        for msg in tqdm(batch, desc="处理消息", leave=False):
            # 只处理有实际内容的消息
            if msg.get("content") and len(msg["content"]) > 5:  # 忽略过短的消息
                classification = classify_with_llm(msg["content"], criteria)
                msg["classification"] = classification
            else:
                msg["classification"] = "TrueNegative"  # 默认短消息为TrueNegative
                
            batch_results.append(msg)
            
        results.extend(batch_results)
        
        if i + batch_size < len(messages):
            time.sleep(delay)  # 批次间延迟
            
    return results

# 主函数
def main():
    # 检查API密钥是否配置
    if SILICONFLOW_API_KEY == "":
        print("警告: 未设置SiliconFlow API密钥。请设置环境变量SILICONFLOW_API_KEY或在代码中直接设置。")
        return
    
    # 确定输入文件
    input_file = "Messages.json"  # 默认使用JSON格式
    if not os.path.exists(input_file):
        print(f"错误: 找不到JSON输入文件 {input_file}")
        return
    
    print(f"使用输入文件: {input_file}")
    
    # 读取数据
    messages = []
    if input_file.endswith(".json"):
        with open(input_file, "r", encoding="utf-8") as f:
            messages = json.load(f)

    
    print(f"读取了 {len(messages)} 条消息")
    
    # 询问用户是否要处理所有消息或仅处理一部分样本
    sample_size = input("请输入要处理的消息数量（输入'all'处理所有消息，或输入一个数字如'100'处理部分消息）: ")
    
    if sample_size.lower() != "all":
        try:
            sample_size = int(sample_size)
            if sample_size < len(messages):
                print(f"将处理 {sample_size} 条消息作为样本")
                messages = messages[:sample_size]
            else:
                print(f"样本大小大于等于总消息数，将处理所有 {len(messages)} 条消息")
        except ValueError:
            print("无效输入，将处理所有消息")
    
    # 批量处理消息
    print("开始处理消息...")
    classified_messages = batch_process_messages(messages)
    
    # 分离TruePositive from TrueNegative
    positive_samples = [msg for msg in classified_messages if msg.get("classification") == "TruePositive"]
    negative_samples = [msg for msg in classified_messages if msg.get("classification") == "TrueNegative"]
    
    print(f"分类完成: TruePositive {len(positive_samples)} 条, TrueNegative {len(negative_samples)} 条")
    
    # 保存结果
    if input_file.endswith(".json"):
        # 保存JSON格式
        with open("positive_samples.json", "w", encoding="utf-8") as f:
            json.dump(positive_samples, f, ensure_ascii=False, indent=2)
        
        with open("negative_samples.json", "w", encoding="utf-8") as f:
            json.dump(negative_samples, f, ensure_ascii=False, indent=2)
    

    
    print("结果已保存到 positive_samples.json/csv 和 negative_samples.json/csv")

if __name__ == "__main__":
    main()