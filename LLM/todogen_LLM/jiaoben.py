import json
from pathlib import Path
import sys
from datetime import timedelta
from dateutil.parser import parse
import mysql.connector
from mysql.connector import Error
from config_loader import get_mysql_config
# 修改导入：导入比较函数，移除旧的合并函数
from compare_data import compare_and_generate_updates

sys.stdout.reconfigure(encoding='utf-8')

def get_db_connection():
    """建立数据库连接"""
    try:
        # current_dir = Path(__file__).parent.absolute()
        # ssl_ca_path = current_dir / "DigiCertGlobalRootCA.crt.pem"

        db_config = get_mysql_config()

        connection = mysql.connector.connect(
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            ssl_ca=db_config['ssl_ca'],
            ssl_disabled=False
        )
        return connection
    except Error as e:
        print(f"❌ 数据库连接失败: {e}")
        return None

def process_end_time(item):
    """处理end_time字段：如果为null则设置为date加1小时"""
    # 确保 date 和 end_time 存在
    date_str = item.get("date")
    end_time_val = item.get("end_time")

    # 检查 end_time 是否为 None 或 "null"
    if end_time_val is None or end_time_val == "null":
        # 检查 date 是否有效
        if date_str and date_str != "null":
            try:
                date_obj = parse(date_str)
                end_time_obj = date_obj + timedelta(hours=1)
                item["end_time"] = end_time_obj.isoformat()
            except (ValueError, TypeError):
                # 如果 date 解析失败，将 end_time 设为 "null"
                item["end_time"] = "null"
        else:
            # 如果 date 无效，将 end_time 设为 "null"
            item["end_time"] = "null"
    # 如果 end_time 已有值，则不做处理
    return item


def insert_to_database(data_list):
    """将处理后的数据插入到数据库"""
    connection = get_db_connection()
    if not connection:
        return False

    if not data_list:
        print("ℹ️ 没有数据需要插入数据库。")
        return True # 没有数据也算成功

    try:
        cursor = connection.cursor()

        # 准备插入SQL - 在字段列表和 VALUES 中加入 message_id
        insert_query = """
        INSERT INTO todolist
        (message_id, user_id, start_time, todo_content, urgency_statu, end_time, location)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # 准备数据 - 字段映射关系：
        # message_id -> message_id  <--- 新增
        # user_id → user_id
        # date → start_time
        # todo_content → todo_content
        # urgency → urgency_statu
        # end_time → end_time
        # location → location
        records_to_insert = []
        skipped_count = 0
        for item in data_list:
            try:
                # 处理日期格式，添加更健壮的错误处理
                start_time = parse(item["date"]).strftime('%Y-%m-%d %H:%M:%S') if item.get("date") and item["date"] != "null" else None
                end_time = parse(item["end_time"]).strftime('%Y-%m-%d %H:%M:%S') if item.get("end_time") and item["end_time"] != "null" else None

                # 确保关键字段存在且不为空
                # 获取 message_id，假设它应该是整数，如果不是或者为空则设为 None 或其他默认值
                message_id_str = item.get("message_id", "")
                message_id = None
                if message_id_str:
                    try:
                        # 假设 message_id 在数据库中是数字类型
                        # 如果 message_id 可能是非数字，需要调整这里的转换逻辑
                        # 或者直接作为字符串插入（如果数据库字段允许）
                        message_id = int(message_id_str)
                    except ValueError:
                         print(f"⚠️ 跳过记录，message_id '{message_id_str}' 不是有效的整数: {item}")
                         skipped_count += 1
                         continue

                user_id = item.get("user_id")
                todo_content = item.get("todo_content")
                urgency = item.get("urgency", "unimportant") # 提供默认值
                location = item.get("location", "") # 提供默认值

                # 增加对 message_id 的检查，如果它在数据库中是必需的
                if user_id is None or todo_content is None or message_id is None: # 假设 message_id 也是必需的
                     print(f"⚠️ 跳过记录，缺少 message_id, user_id 或 todo_content: {item}")
                     skipped_count += 1
                     continue

                # 在 record 元组中加入 message_id
                record = (
                    message_id,     # message_id <--- 新增
                    user_id,        # user_id
                    start_time,     # start_time (可能为 None)
                    todo_content,   # todo_content
                    urgency,        # urgency_statu
                    end_time,       # end_time (可能为 None)
                    location        # location
                )
                records_to_insert.append(record)
            except (ValueError, TypeError, KeyError) as e:
                 print(f"⚠️ 处理记录时出错，已跳过: {item}, 错误: {e}")
                 skipped_count += 1
                 continue


        if skipped_count > 0:
            print(f"ℹ️ 在准备插入数据库时跳过了 {skipped_count} 条记录。")

        if not records_to_insert:
             print("ℹ️ 没有有效记录可供插入数据库。")
             return True # 没有有效数据也算操作完成

        # 执行批量插入
        cursor.executemany(insert_query, records_to_insert)
        connection.commit()
        print(f"✅ 成功尝试插入 {len(records_to_insert)} 条记录到数据库 (受 INSERT IGNORE 影响，实际插入可能更少)")
        return True

    except Error as e:
        print(f"❌ 数据库插入失败: {e}")
        if connection.is_connected():
           connection.rollback() # 如果出错则回滚
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("ℹ️ 数据库连接已关闭。")

# 重命名函数并修改逻辑
def process_and_insert_updates() -> bool:
    """
    从 compare_data 获取更新数据，处理后插入数据库
    :return: 处理和插入是否成功
    """
    try:
        # 1. 调用 compare_data 获取需要处理的数据列表
        print("ℹ️ 开始从 compare_data 获取待处理数据...")
        data_to_process = compare_and_generate_updates()

        if data_to_process is None:
             print("❌ 从 compare_data 获取数据失败。")
             return False

        if not data_to_process:
             print("ℹ️ compare_data 没有返回需要处理的数据。")
             # 即使没有数据，也认为流程是成功的，只是没有工作可做
             # 但仍尝试调用 insert_to_database 以处理空列表情况并关闭连接
             insert_to_database([])
             return True

        print(f"ℹ️ 从 compare_data 成功获取 {len(data_to_process)} 条待处理记录。")

        result_list = []
        for item in data_to_process:
            # 2. 提取和验证字段 (compare_data 返回的结构已基本符合要求)
            # 我们主要需要处理 end_time 和格式化时间
            extracted = {
                "message_id": item.get("message_id", ""), # 保留 message_id 以便调试或未来使用
                "date": item.get("date", "null"),
                "location": item.get("location", ""),
                "end_time": item.get("end_time"), # 先获取原始值
                "todo_content": item.get("todo_content", ""),
                "user_id": item.get("user_id", ""),
                "urgency": item.get("urgency", "unimportant")
            }

            # 3. 处理 end_time 字段
            extracted = process_end_time(extracted)

            # 4. 统一格式化日期字段 (插入数据库时会再次格式化，此步可选，但保持一致性)
            for time_field in ["date", "end_time"]:
                 current_val = extracted.get(time_field)
                 if current_val and current_val != "null":
                    try:
                        # 尝试解析以验证格式，并转为 ISO 格式
                        time_obj = parse(current_val)
                        extracted[time_field] = time_obj.isoformat()
                    except (ValueError, TypeError):
                        # 如果解析失败，标记为 "null"
                        print(f"⚠️ 警告：无法解析字段 '{time_field}' 的值 '{current_val}'，将设为 null。记录：{item}")
                        extracted[time_field] = "null"


            result_list.append(extracted)

        # 5. 将数据插入数据库
        print(f"ℹ️ 准备将处理后的 {len(result_list)} 条记录插入数据库...")
        if not insert_to_database(result_list):
            print("❌ 数据插入数据库失败。")
            return False # 插入失败则整个流程失败

        print("✅ 数据处理和插入流程成功完成。")
        return True

    except Exception as e:
        import traceback
        print(f"❌ 处理和插入过程中发生未预期错误: {str(e)}")
        print(traceback.format_exc()) # 打印详细的回溯信息
        return False

if __name__ == "__main__":
    print("🚀 开始执行数据更新与插入流程...")
    # 执行处理和插入流程
    if process_and_insert_updates():
        print("🎉 流程执行完毕。")
    else:
        print("🔥 处理流程失败，请检查上面的错误信息。")
