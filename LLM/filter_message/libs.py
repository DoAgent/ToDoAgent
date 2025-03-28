from typing import Optional

import yaml
import pymysql
from openai import OpenAI


def read_config(yaml_file):
    """从yaml文件读取配置"""
    with open(yaml_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_db_conn():
    """获取复用数据库链接"""
    config = CONFIG["mysql"]
    conn = pymysql.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        database=config["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,  # 使用字典cursor方便获取数据
    )
    return conn


def execute_sql(sql):
    """执行sql"""
    with DB_CONN.cursor() as cursor:
        cursor.execute(sql)

        # 判断是否为SELECT语句
        if sql.strip().upper().startswith("SELECT"):
            result = cursor.fetchall()
            DB_CONN.commit()  # 提交事务，虽然SELECT语句不需要，但养成习惯
            return result
        else:
            affected_rows = cursor.rowcount
            DB_CONN.commit()  # 提交事务，INSERT/UPDATE/DELETE需要提交
            return affected_rows


def release():
    """释放资源"""
    DB_CONN.close()

    del CONFIG
    del DB_CONN


def get_llm():
    config = CONFIG["openai"]
    client = OpenAI(base_url=config["base_url"], api_key=config["api_key"])

    return client


def send_llm(messages: list[dict[str, str]], model: Optional[str] = None):
    """调用LLM"""

    config = CONFIG["openai"]

    if model is None:
        model = config["model"]

    completion = LLM.chat.completions.create(
        model=model,  # 选择模型
        messages=messages,
        temperature=0,  # 为提高准确率，温度为0
    )

    return completion.choices[0].message.content


def send_llm_with_query(query):
    messages = {
        "role": "user",
        "content": query,
    },
    return send_llm(messages)


def send_llm_with_prompt(query):
    with open("prompt.txt", encoding="utf-8") as f:
        text = f.read()

    messages = [
        {
            "role": "system",
            "content": "你是一个擅长语义分析的短信分析助手，能够从海量的短信息中忽略营销推广、例行通知、余额大于0通知、简单问候，日常聊天"
                       "帮助收件人赛选出有帮助并需要下一步操作的信息。"
                       "特别是会议、开会、生活充值（水电燃气电话费）、账单付款欠款、包裹收取事项。"
                       "对于你无法确定的信息，请优先为需要处理的待办事项。"
                       "对于判断需要代办的事项，输出：事项名称（5-20字，尽量详细直观）、重要程度、计划时间（日期+时间 或尽快）、来源App、关联消息ID",
        },
        {
            "role": "user",
            "content": f"```{text}```，这是一些人工标注数据可用作为参考，其中负样本是可无用信息",
        },
        {
            "role": "user",
            "content": str(query),
        },
    ]

    return send_llm(messages)


###### init #####

CONFIG = read_config("config.yaml")
DB_CONN = get_db_conn()
LLM = get_llm()
