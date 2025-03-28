import time

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
        LIMIT {page_size} OFFSET {offset}
        ;
        """
    #
    #     results.append(execute_sql(sql))
    return execute_sql(sql)
    return results


if __name__ == '__main__':
    resp_list = []
    for i in range(30):
        print(f'正在处理第 {i+1}页数据')
        data  = get_message_with_page(i)
        resp = send_llm_with_prompt(data)
        print('    '+resp)
        print('    ' + '-' * 20)

        resp_list.append(resp)



    print('='* 60)
    print('分批处理完成，开始合并')
    data = send_llm_with_query(f"```{resp_list}```合并内容，使用json进行输出")

    with open(f"data/output_{int(time.time())}.json", "w", encoding="utf-8") as f:
        f.write(data)


