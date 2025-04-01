# main.py
import json
import time
from pathlib import Path
from lib import save_to_mysql,execute_sql, send_llm_with_prompt


def get_message_with_page(page_num, page_size=50):
    """分页查询数据库（保持不变）"""
    offset = page_num * page_size
    sql = f"""
        SELECT content, app_name, message_id, `date`
        FROM Messages
        WHERE app_name in ('com.tencent.mm','SMS','com.ss.android.lark')
        AND DATE(`date`) >= '2025-03-31'
        /* and sender='ASAP Sample' */
        LIMIT {page_size} OFFSET {offset};
    """
    return execute_sql(sql)


def main():
    todo_list = []
    for i in range(0,10):
        print(f'正在处理第 {i + 1}页数据')
        data = get_message_with_page(i)
        if not data:
            print('没有更多数据了')
            break

        resp = send_llm_with_prompt(data)
        resp = resp.replace("```json", "").replace("```", "")
        print('    ' + resp)
        print('    ' + '-' * 20)


        try:
            parsed_resp = json.loads(resp)
            todo_list.extend(parsed_resp)
        except Exception as e:
            print(f"解析响应失败: {e}")

    # 保存到MySQL
    if todo_list:
        save_to_mysql(todo_list)
        print(f"成功保存{len(todo_list)}条数据到数据库")
    else:
        print("没有需要保存的数据")


if __name__ == '__main__':
    main()
