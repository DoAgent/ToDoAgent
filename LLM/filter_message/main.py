import json
import time
from pathlib import Path

from libs import execute_sql, send_llm_with_prompt,send_llm_with_query



def get_message_with_page(page_num,page_size=50):
    """
    分页查询数据库
    """
    results = []

    # for page_num in range(page_count):
    offset = page_num * page_size
    sql = f"""
        SELECT content, app_name, message_id, `date`
        FROM Messages
        WHERE app_name in ('com.tencent.mm' ,'SMS','com.ss.android.lark')
        and DATE(`date`)>='2025-03-21'
        AND (
            content not LIKE '%可用余额%' and
            content not LIKE '%拒收%' and
            content not LIKE '%验证码%' and
            content not LIKE '%话费账单%'
        ) 
        LIMIT {page_size} OFFSET {offset}
        ;
        """
    #
    #     results.append(execute_sql(sql))
    return execute_sql(sql)
    return results


def main():
    todo_list = []
    resp_list = []
    for i in range(30,60):
        print(f'正在处理第 {i+1}页数据')
        data  = get_message_with_page(i)
        if not data:
            print('没有更多数据了')
            break
        resp = send_llm_with_prompt(data)
        print('    '+resp)
        print('    ' + '-' * 20)

        resp_list.append(resp)
        todo_list.extend(json.loads(resp)['todo_list'])


    # data = send_llm_with_query(f"```{resp_list}```合并内容，使用json进行输出")

    Path('data').mkdir(exist_ok=True)
    with open(f"data/output_{int(time.time())}.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(todo_list,ensure_ascii=False,sort_keys=False))

if __name__ == '__main__':
    main()

