import asyncio
from aiomysql import create_pool, DictCursor
from typing import AsyncGenerator, Dict, Any
from tqdm import tqdm   # 设置一个进度条，方便管理数据连接状态
import json
from datetime import datetime
import sys

sys.stdout.reconfigure(encoding='utf-8')

class AsyncDatabaseConfig:
    """异步数据库配置类"""
    def __init__(self, host: str, database: str, password: str):
        self.config = {
            "host": host,
            "port": 3306,
            "db": database,
            "user": "root",
            "password": password,
            "charset": "utf8mb4",
            "cursorclass": DictCursor,
            "autocommit": True
        }

class AsyncDatabaseHandler:
    """异步数据库处理器"""
    def __init__(self, config: AsyncDatabaseConfig):
        self.config = config.config
        self.pool = None

    async def create_pool(self, pool_size: int = 10):
        """创建连接池"""
        try:
            self.pool = await create_pool(
                minsize=1,
                maxsize=pool_size,
                **self.config
            )
            return True
        except Exception as e:
            print(f"连接池创建失败: {e}")
            return False
        

    async def get_total_count(self) -> int:
        """获取消息总数（带缓存）"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) as total FROM Messages")
                result = await cursor.fetchone()
                return result['total'] if result else 0

    async def stream_messages(self, batch_size: int = 100) -> AsyncGenerator[Dict[str, Any], None]:
        """流式获取消息数据"""
        offset = 0
        total = await self.get_total_count()

        with tqdm(total=total, desc="🚀 异步数据流", unit="msg", 
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                    while offset < total:
                        async with self.pool.acquire() as conn:
                            async with conn.cursor() as cursor:
                                try:
                                    await cursor.execute(
                                    f"SELECT * FROM Messages LIMIT {batch_size} OFFSET {offset}"
                                    )
                                    batch = await cursor.fetchall()
                                    if not batch:
                                        break
                                    # 动态更新偏移量
                                    actual_size = len(batch)
                                    offset += actual_size
                                    pbar.update(actual_size)
                            
                                    for row in batch:
                                        yield {
                                        "message_id": str(row["message_id"]),
                                        "user_id": str(row["user_id"]),
                                        "content": row["content"][:500],  # 内存保护
                                        "sender": row.get("sender"),
                                        "app_name": row.get("app_name"),
                                        "date": row["date"].isoformat() if row.get("date") else None,
                                        "urgency": row.get("urgency"),
                                        }
                                except Exception as e:
                                    print(f"🌀 查询异常: {e}")
                                    await asyncio.sleep(1)  # 错误冷却
                            
                    

    async def close(self):
        """关闭连接池"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

async def async_main(host: str, database: str, password: str):
    """异步主函数"""
    db_config = AsyncDatabaseConfig(
        host=host,
        database=database,
        password=password
    )
    
    handler = AsyncDatabaseHandler(db_config)
    if not await handler.create_pool(pool_size=5):
        return {}

    result = {}
    try:
        async for message in handler.stream_messages():
            # 此处可以添加实时处理逻辑
            result[message["message_id"]] = message
            
            # 实时输出进度（每100条更新）
            if len(result) % 100 == 0:
                print(f"\r已处理 {len(result)} 条消息", end="")
                
        return result
    finally:
        await handler.close()

if __name__ == "__main__":
    async def run():
        data = await async_main(
            host="103.116.245.150",
            database="ToDoAgent",
            password="4bc6bc963e6d8443453676"
        )
        # print("\n最终结果示例：")
        # print(json.dumps(
            # dict(list(data.items())[:10]),  
            # ensure_ascii=False, 
            # indent=2
        # ))
    
    asyncio.run(run())