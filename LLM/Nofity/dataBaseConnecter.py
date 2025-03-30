'''
Author: mdhuang555 67590178+mdhuang555@users.noreply.github.com
Date: 2025-03-30 15:57:22
LastEditors: mdhuang555 67590178+mdhuang555@users.noreply.github.com
LastEditTime: 2025-03-30 16:18:55
FilePath: \Notyif\dataBaseConnecter.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
pip install mysql-connector-python
'''
import socket
import json
import mysql.connector
from typing import Dict, Any


class DatabaseConnector:
    def __init__(self, host: str = '103.116.245.150', port: int = 3306):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
    def connect_db(self, db_config: Dict[str, Any]) -> mysql.connector.MySQLConnection:
        """连接到MySQL数据库"""
        try:
            conn = mysql.connector.connect(
                host=db_config.get('host', 'localhost'),
                user=db_config.get('user'),
                password=db_config.get('password'),
                database=db_config.get('database'),
                charset='utf8mb4',  # 使用utf8mb4字符集以支持完整的中文字符
                collation='utf8mb4_unicode_ci'
            )
            return conn
        except Exception as e:
            print(f"数据库连接错误: {e}")
            return None

    def extract_text(self, conn: mysql.connector.MySQLConnection, table: str, column: str) -> list:
        """从指定表格和列中提取文本"""
        try:
            cursor = conn.cursor(dictionary=True)
            # 如果请求所有列，则获取完整的行数据
            if column == '*':
                query = f"SELECT * FROM {table}"
            else:
                query = f"SELECT {column} FROM {table}"
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results  # 返回完整的结果集
        except Exception as e:
            print(f"提取文本错误: {e}")
            return []

    def start_server(self):
        """启动服务器监听请求"""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"服务器启动在 {self.host}:{self.port}")

        while True:
            try:
                client_socket, address = self.server_socket.accept()
                print(f"接受来自 {address} 的连接")
                
                # 接收客户端请求
                data = client_socket.recv(1024).decode('utf-8')
                request = json.loads(data)
                
                # 处理请求
                db_config = {
                    'host': request.get('db_host', 'localhost'),
                    'user': request.get('db_user'),
                    'password': request.get('db_password'),
                    'database': request.get('database')
                }
                table = request.get('table')
                column = request.get('column')
                
                # 连接数据库并提取文本
                conn = self.connect_db(db_config)
                if conn:
                    try:
                        results = self.extract_text(conn, table, column)
                        response = {'status': 'success', 'data': results}
                    except Exception as e:
                        response = {'status': 'error', 'message': str(e)}
                    finally:
                        conn.close()
                else:
                    response = {'status': 'error', 'message': '数据库连接失败'}
                
                # 发送响应
                response_data = json.dumps(response, ensure_ascii=False)
                response_bytes = response_data.encode('utf-8')
                
                # 先发送数据长度
                length_prefix = len(response_bytes).to_bytes(4, byteorder='big')
                client_socket.send(length_prefix)
                
                # 再发送实际数据
                client_socket.send(response_bytes)
                client_socket.close()
                
            except Exception as e:
                print(f"处理请求错误: {e}")
                continue

if __name__ == "__main__":
    server = DatabaseConnector()
    server.start_server()
