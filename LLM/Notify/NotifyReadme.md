<!--
 * @Description: 
 * @Author: Manda
 * @Version: 
 * @Date: 2025-03-30 17:01:58
 * @LastEditors: Manda
 * @LastEditTime: 2025-04-03 15:22:55
-->

## 20250320 
一些做RAG的数据文档以user_id来命名

### dataBaseConnecter
dataBaseConnecter.py实现连接服务器功能，并提供端口让其他py（如db2txt.py）将指定数据库内文本提取出来

### db2txt （好像直接在数据库完成了对比，这个文件貌似没啥用了）
db2txt.py 将ToDoAgent数据库中的ToDoList表格内容下载到txt中，按ToDoList表格中的user_id来命名txt,也就是不同的user_id有不同的txt

### usrSpareTime -->千人千面推送时间可以用到的RAG
usrSpareTime.py 将ToDoAgent数据库中的UCtodolist表格内容last_modified，数据获取出来分36个时段进行统计，统计出出现频率最高的6个时段， 将时段信息及出现次数下载到txt中，以相同“todo_id”为前提，查询ToDoList表格中的user_id来命名txt,也就是不同的user_id有不同的txt

### compareDb2txt-->自动生成ToDoList可以用到的的RAG
compareDb2txt.py 将ToDoAgent数据库中的UCtodolist表格内容与ToDoList做对比，以相同“todo_id”为前提，对比“start_time”"end_time""location""todo_content",一旦发现有差异,则将差异内容下载到txt中，按ToDoList表格中的user_id来命名txt,也就是不同的user_id有不同的txt

