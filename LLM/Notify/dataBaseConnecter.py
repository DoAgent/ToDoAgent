# '''
# Author: mdhuang555 67590178+mdhuang555@users.noreply.github.com
# Date: 2025-03-30 15:57:22
# LastEditors: mdhuang555 67590178+mdhuang555@users.noreply.github.com
# LastEditTime: 2025-04-03 11:32:30
# FilePath: \Notyif\dataBaseConnecter.py
# Description: 数据库连接器，支持SSL连接
# '''
import socket
import json
import mysql.connector
from typing import Dict, Any, Optional
import yaml
from pathlib import Path

class DatabaseConnector:
    def __init__(self, host: str = '103.116.245.150', port: int = 3306):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            config_path = Path(__file__).parent / "config.yaml"
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"加载配置文件错误: {e}")
            return {}
        
    def connect_db(self) -> Optional[mysql.connector.MySQLConnection]:
        """连接到MySQL数据库，使用SSL连接"""
        try:
            # 获取SSL证书路径
            current_dir = Path(__file__).parent.absolute()
            ssl_ca_path = current_dir / "DigiCertGlobalRootCA.crt.pem"
            
            # 确保SSL证书文件存在
            if not ssl_ca_path.exists():
                raise FileNotFoundError(f"SSL证书文件未找到: {ssl_ca_path}")
            
            # 建立数据库连接
            conn = mysql.connector.connect(
                host=self.config["mysql"]["host"],
                port=self.config["mysql"].get("port", 3306),
                user=self.config["mysql"]["user"],
                password=self.config["mysql"]["password"],
                database=self.config["mysql"]["database"],
                ssl_ca=str(ssl_ca_path),
                ssl_disabled=False,
                charset='utf8mb4',
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
            return results
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
                table = request.get('table')
                column = request.get('column')
                
                # 连接数据库并提取文本
                conn = self.connect_db()
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
