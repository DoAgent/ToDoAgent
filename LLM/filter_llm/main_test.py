from io import StringIO
from pathlib import Path
import shutil

import pandas as pd
from libs import *


def send_llm_with_query(query):
    system = """
# 角色
你是一个专业的短信内容分析助手，根据输入判断内容的类型及可信度，为用户使用信息提供依据和便利。

# 任务
对于输入的多条数据，分析每一条数据内容（主键：`message_id`）属于【物流取件、缴费充值、待付(还)款、会议邀约、其他】的可能性百分比。
内容只有包含“邀请你加入飞书视频会议”才算会议邀请，其他就不算会议邀请。
主要对于聊天、问候、回执、结果通知、上月账单等信息不需要收件人进行下一步处理的信息，直接归到其他类进行忽略

# 要求
1. 以csv格式输出

# 输出示例
```
"message_id","物流取件","欠费缴纳","待付(还)款","会议邀约","其他","分类"
1111111,99,11,10,1,0,0,"物流取件"
2222222,0,0,45,60,0,"日常聊天"
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


def main():
    df = pd.read_csv("data.csv")
    filtered_df = df
    # filtered_df = df[(df['message_id'] >= 321123840) & (df['message_id'] <= 321125971)]

    data_list = filtered_df.to_dict('records')  # 转换为字典列表
    batch_size = 30
    all_associated_data = []
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i + batch_size]
        print(f"处理第 {i // batch_size + 1} / {len(data_list) // batch_size} 批数据, 共 {len(batch)} 条")

        resp_data = send_llm_with_query(batch)
        resp_data = resp_data.replace("```csv", "").replace("```", "")
        batch_df = pd.read_csv(StringIO(resp_data))
        print('-' * 20)
        print(batch_df)
        batch_df.to_csv(f"debug/{i}.csv")

        all_associated_data.append(batch_df)

    df_associated = pd.concat(all_associated_data, ignore_index=True)
    df_final = filtered_df.merge(df_associated, on="message_id", how="left")

    print(df_final)
    df_final.to_csv('data_by_llm.csv')


if __name__ == '__main__':
    shutil.rmtree('debug', ignore_errors=True)
    Path('debug').mkdir()
    main()
    df = pd.read_csv("data_by_llm.csv")
    print(123)
