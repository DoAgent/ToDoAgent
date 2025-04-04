
# '''
# Description: 定时运行数据库相关脚本的主程序
# Author: Manda
# Version: 1.0
# Date: 2024-03-30
# '''
import schedule
import time
import subprocess
import logging
from datetime import datetime
import os
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('notify_main.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def run_script(script_name: str):
    """运行指定的Python脚本"""
    try:
        logging.info(f"开始运行脚本: {script_name}")
        result = subprocess.run(['python', script_name], 
                              capture_output=True, 
                              text=True,
                              encoding='utf-8')  # 明确指定使用 UTF-8 编码
        
        if result.returncode == 0:
            logging.info(f"脚本 {script_name} 运行成功")
            if result.stdout:
                logging.info(f"输出: {result.stdout}")
        else:
            logging.error(f"脚本 {script_name} 运行失败")
            if result.stderr:
                logging.error(f"错误: {result.stderr}")
    except Exception as e:
        logging.error(f"运行脚本 {script_name} 时发生错误: {str(e)}")


def run_all_scripts():
    """运行所有脚本"""
    logging.info("开始执行所有脚本")
    
    # 获取当前脚本所在的目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    scripts = ['db2txt.py', 'usrSpareTime.py', 'compareDb2txt.py']
    
    for script in scripts:
        # 使用完整的文件路径
        script_path = os.path.join(current_dir, script)
        if os.path.exists(script_path):
            run_script(script_path)
            time.sleep(5)
        else:
            logging.error(f"脚本文件不存在: {script_path}")
    
    logging.info("所有脚本执行完成")

def main():
    logging.info("启动定时任务程序")
    
    # 设置每小时运行一次
    schedule.every().hour.at(":43").do(run_all_scripts)
    
    # 立即运行一次
    run_all_scripts()
    
    # 持续运行
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logging.error(f"运行时发生错误: {str(e)}")
            time.sleep(60)  # 发生错误时等待一分钟后继续

if __name__ == "__main__":
    main() 