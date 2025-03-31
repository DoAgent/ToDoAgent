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


def send_llm(messages: list[dict[str, str]], model: Optional[str] = None, resp_json=False):
    """调用LLM"""

    config = CONFIG["openai"]

    if model is None:
        model = config["model"]

    if resp_json:
        completion = LLM.chat.completions.create(
            model=model,  # 选择模型
            messages=messages,
            temperature=0,  # 为提高准确率，温度为0
            response_format={ "type": "json_object" },
        )
    else:
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
    system = """
    # 角色
    你是一个专业的短信内容分析助手，根据输入判断内容的类型及可信度，为用户使用信息提供依据和便利。

    # 任务
    对于输入的多条数据，分析每一条数据内容（主键：`message_id`）属于【物流取件、缴费充值、待付(还)款、会议邀约、其他】的可能性百分比。
    内容只有包含“邀请你加入飞书视频会议”才算会议邀请，其他就不算会议邀请。
    主要对于聊天、问候、回执、结果通知、上月账单等信息不需要收件人进行下一步处理的信息，直接归到其他类进行忽略

    # 要求
    1. 以json格式输出

    # 输出示例
    ```
    [
        {"message_id":"1111111",  "物流取件":99,"欠费缴纳":11, "待付(还)款":1: "会议邀约":0,"其他":0, "分类":"物流取件"},
        {"message_id":"222222",  "物流取件":0,"欠费缴纳":0, "待付(还)款":1: "会议邀约":0,"其他":99, "分类":"分类"}
    ]

    ```
        """

    with open("prompt.txt", encoding="utf-8") as f:
        tag_prompt = f.read()

    messages = [
        {
            "role": "system",
            "content": system,
        },
        {
            "role": "user",
            "content": f"```{tag_prompt}```这是一些人工标注数据，其中负样本表示`其他`类，接下来我会输入本次要处理的数据：",
        },
        {
            "role": "user",
            "content": str(query),
        },
    ]
    return send_llm(messages)

def save_to_mysql(data):
    """新增：保存数据到MySQL"""
    # 字段映射关系（中文键名 → 数据库英文列名）
    COLUMN_MAPPING = {
        "message_id": "message_id",
        "物流取件": "logistics_pickup",
        "欠费缴纳": "overdue_payment",
        "待付(还)款": "pending_payment",
        "会议邀约": "meeting_invitation",
        "其他": "other",
        "分类": "category"
    }

    # 连接 MySQL 数据库
    conn =get_db_conn()

    try:
        with conn.cursor() as cursor:
            # 构造插入 SQL
            sql = f"""
            INSERT INTO TrialMessages 
            ({', '.join(COLUMN_MAPPING.values())})
            VALUES ({', '.join(['%s'] * len(COLUMN_MAPPING))})
            """

            # 转换数据格式（按映射顺序提取值）
            values = []
            for item in data:
                row = [item[key] for key in COLUMN_MAPPING.keys()]
                values.append(row)

            # 批量插入
            cursor.executemany(sql, values)
            conn.commit()
            print(f"成功插入 {cursor.rowcount} 条数据")

    except Exception as e:
        conn.rollback()
        print(f"数据插入失败: {e}")

    finally:
        conn.close()

###### init #####

CONFIG = read_config("config.yaml")
DB_CONN = get_db_conn()
LLM = get_llm()
