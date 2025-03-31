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
    with open("prompt.txt", encoding="utf-8") as f:
        text = f.read()

    system = '''
你是一个专业的短信事项提取助手，我将提供给你一组短信数组，请严格按照以下规则从短信数据中提取TODO事项：
# 提取原则
只提取需要用户执行后续操作的以下类型事项：
- 会议
- 充值缴费
- 账单还款
- 快递收取
# 特殊要求
凡是出现'日程邀请'或者'邀请你加入视频会议'或者'发起一个视频会议'都属于会议

# 必须过滤的情况
❌ 营销推广（含链接/优惠信息）
❌ 聊天问候（节日祝福/天气提醒）
❌ 状态通知（账单已支付/快递已签收）


# 输出格式（JSON）
{
    "todo_list": [
        {
            "todo": "事项名称，具体行动+关键要素（如还款金额/会议时间/快递公司）",
            "level": "重要程度，紧急（有截止时间且<24h）/重要/常规",
            "type":"会议/充值缴费/账单还款/快递收取",
            "time": "事件具体时间（按YYYY-MM-DD HH:mm转换），若无则'尽快'",
            "app_name": "原始app_name",
            "message_id": "原始消息唯一标识message_id"
        },
        ...
    ]
}

# 输入
我将提供给你一组短信json数组，里面包括了app_name、message_id、content、date等字段，请你根据以上规则进行分析，输出json格式的结果。

# 输入示例
正向案例："【菜鸟驿站】取件码7781，顺丰快递请19:00前至3号楼快递柜取件"
负面案例："【中国银行】您尾号8879的账户余额1829.34元"（过滤余额通知）


```

'''

    messages = [
        {
            "role": "system",
            "content": system,
        },
        {
            "role": "user",
            "content": f"```{text}```，这是一些人工标注数据可进作为进一步参考",
        },
        {
            "role": "user",
            "content": "这是本次要处理的数据:"+str(query),
        },
    ]

    return send_llm(messages,resp_json=True)


###### init #####

CONFIG = read_config("config.yaml")
DB_CONN = get_db_conn()
LLM = get_llm()
