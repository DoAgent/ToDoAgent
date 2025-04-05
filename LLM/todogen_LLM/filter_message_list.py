import mysql.connector
from pathlib import Path
import sys
from config_loader import get_mysql_config

sys.stdout.reconfigure(encoding='utf-8')

def get_message_ids():
    current_dir = Path(__file__).parent.absolute()
    # ssl_ca_path = current_dir / "DigiCertGlobalRootCA.crt.pem"
    # message_ids = []
    config = get_mysql_config()

    try:
        # 建立数据库连接
        cnx = mysql.connector.connect(
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
            database=config['database'],
            ssl_ca=config['ssl_ca'],
            ssl_disabled=False
        )

        cursor = cnx.cursor()
        
        # 执行查询
        cursor.execute("SELECT message_id FROM filter_message_test")
        results = cursor.fetchall()
        
        # 提取为纯数字列表
        message_ids = [row[0] for row in results]
        
        cursor.close()
        cnx.close()
        print(f"成功获取 {len(message_ids)} 条message_id")
        
    except mysql.connector.Error as err:
        print(f"数据库错误: {err}")
    except Exception as e:
        print(f"发生异常: {str(e)}")
    
    return message_ids

if __name__ == '__main__':
    id_list = get_message_ids()
    print("\n提取结果：")
    print(id_list)