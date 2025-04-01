test test


# ToDoAgent
Microsoft Azure Enabled LLM Agent that helps you read apps notifications, emails, etc. Then generate a ToDo list with key person and deadline. The future version may directly help you finish the things on the generated ToDoList

# What's New in the APP
The current software version V0.0.8, mobile database version V0.0.4, cloud database version V0.0.3, now we have two more significant updates, one is pleased to tell you that you are now free to control which apps can be included in the information, in the Settings -> Notification Settings you can view the recent notification of harassment of the apps you can turn off in accordance with their wishes! You can turn off or turn on the permission to get notifications as you wish, of course, to let the server get the permission to get your notifications. We pay special attention to user privacy, so only apps that have pushed messages will appear in the list. Secondly, it is now possible to manually update calendar notifications. Tap the little calendar next to the to-do list and you'll be surprised to find that, oops, the system calendar has an extra substitute reminder, that's pretty cool, isn't it? I know that's really cool!

# Connecting to the Database
Starting from now, we can connect to the database, and also support WeChat groups and SMS.

# API for Developers

本部分详细介绍了 ToDoAgent 项目的所有 API 接口、调用方法和示例。系统提供了两种类型的 API：普通 API 和加密 API，以满足不同的安全需求。

## 目录

- [API 概述](#api-概述)
- [普通 API](#普通-api)
  - [消息 API](#消息-api)
  - [API 配置](#api-配置)
- [加密 API](#加密-api)
  - [获取公钥](#获取公钥)
  - [加密消息 API](#加密消息-api)
  - [加密 API 配置](#加密-api-配置)
- [数据模型](#数据模型)
- [加密机制](#加密机制)
- [错误处理](#错误处理)
- [示例代码](#示例代码)

## API 概述

系统提供两种类型的 API 接口：

1. **普通 API**：直接传输 JSON 数据，适用于内部网络或不敏感数据。
2. **加密 API**：使用 RSA+AES 混合加密方式保护数据传输，适用于敏感数据或公网环境。

所有 API 都遵循 RESTful 设计原则，使用 HTTP 状态码表示请求结果，并提供统一的响应格式。

## 普通 API

### 基本请求格式

所有普通 API 请求都使用以下格式：

```json
{
  "type": "操作类型",
  "table": "目标表",
  "id": 可选的ID参数,
  "data": 可选的数据对象
}
```

其中：
- `type`: 操作类型，可选值包括 `get_all`, `get_by_id`, `create`, `update`, `delete`
- `table`: 目标表，可选值包括 `messages`, `apiconfig`
- `id`: 用于 `get_by_id`, `update`, `delete` 操作的 ID 参数
- `data`: 用于 `create` 和 `update` 操作的数据对象

### 基本响应格式

所有普通 API 响应都使用以下格式：

```json
{
  "success": true/false,
  "statusCode": HTTP状态码,
  "message": "响应消息",
  "data": 响应数据,
  "timestamp": 时间戳,
  "path": "请求路径"
}
```

### 消息 API

#### 获取所有消息

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "get_all",
  "table": "messages"
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 200,
  "message": "操作成功",
  "data": [
    {
      "id": 1,
      "sender": "系统",
      "content": "欢迎使用ToDoAgent",
      "message_id": null,
      "user_id": 1
    },
    
  ],
  "timestamp": 1621234567890
}
```

#### 获取单个消息

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "get_by_id",
  "table": "messages",
  "id": 1
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 200,
  "message": "操作成功",
  "data": {
    "id": 1,
    "sender": "系统",
    "content": "欢迎使用ToDoAgent",
    "message_id": null,
    "user_id": 1
  },
  "timestamp": 1621234567890
}
```

#### 创建消息

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "create",
  "table": "messages",
  "data": {
    "sender": "用户",
    "content": "这是一条新消息",
    "message_id": null,
    "user_id": 1
  }
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 201,
  "message": "创建成功",
  "data": {
    "id": 2,
    "sender": "用户",
    "content": "这是一条新消息",
    "message_id": null,
    "user_id": 1
  },
  "timestamp": 1621234567890
}
```

#### 更新消息

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "update",
  "table": "messages",
  "id": 2,
  "data": {
    "content": "这是更新后的消息内容"
  }
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 200,
  "message": "操作成功",
  "data": {
    "id": 2,
    "sender": "用户",
    "content": "这是更新后的消息内容",
    "message_id": null,
    "user_id": 1
  },
  "timestamp": 1621234567890
}
```

#### 删除消息

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "delete",
  "table": "messages",
  "id": 2
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 204,
  "message": "删除成功",
  "timestamp": 1621234567890
}
```

### API 配置

#### 获取所有 API 配置

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "get_all",
  "table": "apiconfig"
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 200,
  "message": "操作成功",
  "data": [
    {
      "id": 1,
      "api_key": "sk-xxxxxxxxxxxx",
      "endpoint": "https://api.openai.com/v1",
      "model_name": "gpt-4"
    },
    // 更多配置...
  ],
  "timestamp": 1621234567890
}
```

#### 获取单个 API 配置

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "get_by_id",
  "table": "apiconfig",
  "id": 1
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 200,
  "message": "操作成功",
  "data": {
    "id": 1,
    "api_key": "sk-xxxxxxxxxxxx",
    "endpoint": "https://api.openai.com/v1",
    "model_name": "gpt-4"
  },
  "timestamp": 1621234567890
}
```

#### 创建 API 配置

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "create",
  "table": "apiconfig",
  "data": {
    "api_key": "sk-xxxxxxxxxxxx",
    "endpoint": "https://api.openai.com/v1",
    "model_name": "gpt-4"
  }
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 201,
  "message": "创建成功",
  "data": {
    "id": 2,
    "api_key": "sk-xxxxxxxxxxxx",
    "endpoint": "https://api.openai.com/v1",
    "model_name": "gpt-4"
  },
  "timestamp": 1621234567890
}
```

#### 更新 API 配置

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "update",
  "table": "apiconfig",
  "id": 2,
  "data": {
    "model_name": "gpt-3.5-turbo"
  }
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 200,
  "message": "操作成功",
  "data": {
    "id": 2,
    "api_key": "sk-xxxxxxxxxxxx",
    "endpoint": "https://api.openai.com/v1",
    "model_name": "gpt-3.5-turbo"
  },
  "timestamp": 1621234567890
}
```

#### 删除 API 配置

- **URL**: `/api`
- **方法**: POST
- **请求体**:

```json
{
  "type": "delete",
  "table": "apiconfig",
  "id": 2
}
```

- **响应示例**:

```json
{
  "success": true,
  "statusCode": 204,
  "message": "删除成功",
  "timestamp": 1621234567890
}
```

## 加密 API

加密 API 使用 RSA+AES 混合加密方式保护数据传输，适用于敏感数据或公网环境。

### 加密机制概述

1. 客户端首先获取服务器的 RSA 公钥
2. 客户端使用 RSA 公钥加密 AES 密钥
3. 客户端使用 AES 密钥加密实际数据
4. 服务器使用 RSA 私钥解密 AES 密钥
5. 服务器使用解密后的 AES 密钥解密实际数据
6. 服务器处理请求并使用相同的方式加密响应

### 获取公钥

- **URL**: `/api/encrypted/publickey`
- **方法**: GET
- **响应**: Base64 编码的 RSA 公钥

### 加密请求格式

所有加密 API 请求都使用以下格式：

```json
{
  "encryptedData": {
    "encryptedData": "AES加密的数据",
    "encryptedKey": "RSA加密的AES密钥"
  }
}
```

### 加密响应格式

所有加密 API 响应都使用以下格式：

```json
{
  "encryptedData": {
    "encryptedData": "AES加密的数据",
    "encryptedKey": "RSA加密的AES密钥"
  }
}
```

### 加密消息 API

#### 获取所有消息（加密）

- **URL**: `/api/encrypted/messages?type=get_all`
- **方法**: POST
- **请求体**: 加密请求格式（无需加密任何数据，只需提供空的加密请求）
- **响应**: 加密响应格式，解密后为消息列表的 JSON 字符串

#### 获取单个消息（加密）

- **URL**: `/api/encrypted/messages?type=get_by_id&id=1`
- **方法**: POST
- **请求体**: 加密请求格式（无需加密任何数据，只需提供空的加密请求）
- **响应**: 加密响应格式，解密后为单个消息的 JSON 字符串

#### 获取用户消息（加密）

- **URL**: `/api/encrypted/messages?type=get_by_user_id&id=1`
- **方法**: POST
- **请求体**: 加密请求格式（无需加密任何数据，只需提供空的加密请求）
- **响应**: 加密响应格式，解密后为用户消息列表的 JSON
