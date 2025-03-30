from database_of_messages import main as db_main, DateTimeEncoder
from typing import List, Dict, Any, Optional
import json
import os
from dotenv import load_dotenv

from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import UserMessage
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizableTextQuery

# Load environment variables of LLM api (if .env file is present)
load_dotenv()

# 配置FREE/Azure Phi4 API
PHI4_FREE_KEY = os.getenv("PHI4_FREE_KEY", "")
PHI4_FREE_ENDPOINT = os.getenv("PHI4_FREE_ENDPOINT", "")
PHI4_FREE_DEPLOYMENT = "ollama"

PHI4_AZURE_ENDPOINT = os.getenv("PHI4_AZURE_ENDPOINT", "")
PHI4_AZURE_API_KEY = os.getenv("PHI4_AZURE_API_KEY", "")
PHI4_AZURE_DEPLOYMENT = "Azure"

# 配置Search API
SEARCH_GOOGLE_ENDPOINT = os.getenv("SEARCH_GOOGLE_ENDPOINT","")
SEARCH_GOOGLE_INDEX = os.getenv("SEARCH_GOOGLE_INDEX","")
SEARCH_GOOGLE_KEY = os.getenv("SEARCH_GOOGLE_KEY", "")

SEARCH_AZURE_ENDPOINT = os.getenv("SEARCH_AZURE_ENDPOINT","")
SEARCH_AZURE_INDEX = os.getenv("SEARCH_AZURE_INDEX","")
SEARCH_AZURE_KEY = os.getenv("SEARCH_AZURE_KEY", "")

# Placeholder for phi4 to avoid "not defined" error
class phi4:
    api_type = None
    api_endpoint = None
    api_key = None
    api_version = None

class search:
    api_endpoint = None
    api_index = None
    api_key = None
    api_provider = None

# 如果有FREE PHI4配置，则使用FREE PHI4
if PHI4_FREE_KEY.strip() and PHI4_FREE_ENDPOINT.strip() and PHI4_FREE_DEPLOYMENT.strip():
    phi4.api_type = PHI4_FREE_DEPLOYMENT
    phi4.api_endpoint = PHI4_FREE_ENDPOINT
    phi4.api_key = PHI4_FREE_KEY
    phi4.api_version = "2025-05-15"
else:
    phi4.api_type = PHI4_AZURE_DEPLOYMENT
    phi4.api_endpoint = PHI4_AZURE_ENDPOINT  # Correct fallback
    phi4.api_key = PHI4_AZURE_API_KEY       # Correct fallback
    phi4.api_version = "2023-05-15"

print(f"Using {phi4.api_type} API with endpoint: {phi4.api_endpoint}")

# 如果有GOOGLE SEARCH配置，则使用GOOGLE SEARCH
if SEARCH_GOOGLE_KEY.strip() and SEARCH_GOOGLE_ENDPOINT.strip():
    search.api_endpoint = SEARCH_GOOGLE_ENDPOINT
    search.api_index = SEARCH_GOOGLE_INDEX
    search.api_key = SEARCH_GOOGLE_KEY
    search.api_provider = "Google"
else:
    search.api_endpoint = SEARCH_AZURE_ENDPOINT
    search.api_index = SEARCH_AZURE_INDEX
    search.api_key = SEARCH_AZURE_KEY
    search.api_provider = "Azure"
# 其他情況、则使用AZURE SEARCH
    
print(f"Using {search.api_provider} Search API with endpoint: {search.api_endpoint}")


# UsePhi4RAG_Step 1: Connect to Azure AI Inference & Azure AI Search
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import (
    SystemMessage,
    UserMessage,
    TextContentItem,
    ImageContentItem,
    ImageUrl,
    ImageDetailLevel,
)
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizableTextQuery

chat_client = ChatCompletionsClient(
    endpoint=phi4.api_endpoint,  # Use the value directly
    credential=AzureKeyCredential(phi4.api_key),  # Use the value directly
)

search_client = SearchClient(
    endpoint=search.api_endpoint,  # Use the value directly
    index_name=search.api_index,  # Use the value directly
    credential=AzureKeyCredential(search.api_key),  # Use the value directly
)



class MessageProcessor:
    """消息处理器类，用于从数据库获取和过滤消息"""
    
    def __init__(self, host: Optional[str] = None, database: Optional[str] = None, password: Optional[str] = None):
        """
        初始化消息处理器
        
        参数:
            host: 数据库主机地址，默认从环境变量获取
            database: 数据库名称，默认从环境变量获取
            password: 数据库密码，默认从环境变量获取
        """
        self.host = host or os.getenv('DB_HOST', '103.116.245.150')
        self.database = database or os.getenv('DB_NAME', 'ToDoAgent')
        self.password = password or os.getenv('DB_PASSWORD', '4bc6bc963e6d8443453676')
        self.all_data = None
    
    def fetch_all_messages(self) -> List[Dict[str, Any]]:
        """
        从数据库获取所有消息
        
        返回:
            所有消息的列表
        """
        self.all_data = db_main(
            host=self.host,
            database=self.database,
            password=self.password
        )
        return self.all_data
    
    # UsePhi4RAG_Step 2: Retrieve relevant documents from Azure AI Search
    def retrieve_documents(query: str, top: int = 3):
        vector_query = VectorizableTextQuery(text=query, k_nearest_neighbors=top, fields="text_vector")
        results = search_client.search(search_text=query, vector_queries=[vector_query], select=["content"], top=top)    
        return [doc["content"] for doc in results]

        ## Example for Muli-modal search if you have a text_vector AND image_vector field in your vector_index
        ## NOTE, image vectorization is in preview at the time of writing this code, please use azure-search-documents pypi version >11.6.0b6 
        # def retrieve_documents_multimodal(query: str, image_url: str, top: int = 3):
        #     text_vector_query = VectorizableTextQuery(
        #         text=query,
        #         k_nearest_neighbors=top,
        #         fields="text_vector",
        #         weight=0.5  # Adjust weight as needed
        #     )
        #     image_vector_query = VectorizableImageUrlQuery(
        #         url=image_url,
        #         k_nearest_neighbors=top,
        #         fields="image_vector",
        #         weight=0.5  # Adjust weight as needed
        #     )

        #     results = search_client.search(
        #         search_text=query,  
        #         vector_queries=[text_vector_query, image_vector_query],
        #         select=["content"],
        #         top=top
        #     )
        #     return [doc["content"] for doc in results]

    # UsePhi4RAG_Step 3: Generate a multimodal RAG-based answer using retrieved text and an image input
    def generate_multimodal_rag_response(query: str, image_url: str):
        # Retrieve text context from search
        docs = retrieve_documents(query)
        context = "\n---\n".join(docs)

        # Build a prompt that combines the retrieved context with the user query
        prompt = f"""You are a helpful assistant. Use only the following context to answer the question. If the answer isn't in the context, say 'I don't know'.
        Context: {context} Question: {query} Answer:"""
        # Create a chat request that includes both text and image input
        response = chat_client.complete(
            messages=[
                SystemMessage(content="You are a helpful assistant that can process both text and images."),
                UserMessage(
                    content=[
                        TextContentItem(text=prompt),
                        ImageContentItem(image_url=ImageUrl(url=image_url, detail=ImageDetailLevel.HIGH)),
                    ]
                ),
            ]
        )
        return response.choices[0].message.content

    
    def filter_messages_by_ids(self, target_ids: List[int], data: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        根据目标ID过滤消息数据
        
        参数:
            target_ids: 目标消息ID列表
            data: 要过滤的数据，默认使用已获取的数据
        返回:
            过滤后的消息列表，每个消息包含完整信息
        """
        
        # 如果没有提供数据且没有已获取的数据，则获取所有消息
        if data is None:
            if self.all_data is None:
                self.fetch_all_messages()
            data = self.all_data
        
        ##########################################FilterLLM Start############################################
        
        # Sematic Filting: MVP use case prompt提示词
        # string FilterLLM_MVPprompt = " "
        ICL_prompt = "You  are a notification filter and need to only leave message_id  in your output. and make sure they are " \
        "within 3 kinds of notification: (1) about express delivery, only leave its message id when it is asking me to pick up " \
        "(2) about bill charging, only leave its message id when it is asking me  to pay it (3) about conference notification," \
        " only leave its message id when it is asking me to join. should only output ‘message_id’  such as ’1, 3‘, and its " \
        "content, and your reason."
        RAG_prompt = ""
        raw_notification = data

        
        user_query = ICL_prompt + json.dumps(raw_notification, ensure_ascii=False, indent=4, cls=DateTimeEncoder) + RAG_prompt
        sample_image_url = "https://images.unsplash.com/photo-1542291026-7eec264c27ff?q=80&w=1770&auto=format&fit=crop&ixlib=rb-4.0.3"
        answer = self.generate_multimodal_rag_response(user_query, sample_image_url)
        print(f"Q: {user_query}\nA: {answer}")

        
        

        ##########################################FilterLLM End############################################

        result = []
        for msg in data:
            msg_id = msg.get('message_id')
            if msg_id is not None and msg_id in target_ids:
                # 返回完整的消息字典
                result.append(msg)
        return result
    
    def process_messages(self, target_ids: List[int]) -> List[Dict[str, Any]]:
        """
        处理指定ID的消息
        
        参数:
            target_ids: 目标消息ID列表
        返回:
            过滤后的消息列表
        """
        # 确保已获取数据
        if self.all_data is None:
            self.fetch_all_messages()
        
        # 过滤消息
        filtered_result = self.filter_messages_by_ids(target_ids)
        
        return filtered_result
    
    def to_json(self, data: List[Dict[str, Any]]) -> str:
        """
        将数据转换为JSON字符串
        
        参数:
            data: 要转换的数据
        返回:
            格式化的JSON字符串
        """
        return json.dumps(data, ensure_ascii=False, indent=4, cls=DateTimeEncoder)


class DataTransformer:
    """数据转换器类，用于转换和处理数据格式"""
    
    @staticmethod
    def rename_date_to_start_time(data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        将数据列表中的date字段重命名为start_time
        
        参数:
            data_list: 包含消息数据的字典列表
        返回:
            处理后的数据列表
        """
        result = []
        for item in data_list:
            # 创建一个新的字典，避免修改原始数据
            new_item = item.copy()
            if 'date' in new_item:
                # 将date字段的值赋给新的start_time字段
                new_item['start_time'] = new_item['date']
                # 删除原来的date字段
                del new_item['date']
            result.append(new_item)
        return result
    
    @staticmethod
    def combine_content_and_start_time(data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        将content和start_time字段整合为一个新的content字段
        
        参数:
            data_list: 包含消息数据的字典列表
        返回:
            处理后的数据列表
        """
        result = []
        for item in data_list:
            # 创建一个新的字典，避免修改原始数据
            new_item = item.copy()
            if 'content' in new_item and 'start_time' in new_item:
                # 保存原始content
                original_content = new_item['content']
                # 整合content和start_time
                new_item['content'] = f"起始于{new_item['start_time']}，{original_content}"
                # 删除start_time字段
                del new_item['start_time']
            result.append(new_item)
        return result
    
    @staticmethod
    def transform_data(data_list: List[Dict[str, Any]], rename_date: bool = True, combine_fields: bool = False) -> List[Dict[str, Any]]:
        """
        转换数据，可以选择是否重命名date字段和是否合并字段
        
        参数:
            data_list: 要转换的数据列表
            rename_date: 是否将date重命名为start_time
            combine_fields: 是否合并content和start_time字段
        返回:
            转换后的数据列表
        """
        result = data_list
        
        if rename_date:
            result = DataTransformer.rename_date_to_start_time(result)
        
        if combine_fields:
            result = DataTransformer.combine_content_and_start_time(result)
        
        return result







if __name__ == '__main__':
    """主函数示例"""
    # 创建消息处理器实例
    processor = MessageProcessor()
    
    # 定义目标消息ID
    target_ids = [
        318002613,
        318003223,
        318002211
    ]
    
    # 处理消息
    result = processor.process_messages(target_ids)
    
    # 使用数据转换器转换数据
    transformer = DataTransformer()
    transformed_data = transformer.transform_data(result, rename_date=True, combine_fields=True)
    
    # 打印结果
    print(processor.to_json(transformed_data))

