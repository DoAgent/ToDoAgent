<!--
 * @Description: 
 * @Author: Manda
 * @Version: 
 * @Date: 2025-03-16 16:50:53
 * @LastEditors: Manda
 * @LastEditTime: 2025-03-16 16:52:52
-->
# 项目结构说明 (Project Structure)

## 主要目录结构 (Main Directory Structure) 

com.asap.todoexmple/
├── activity/
│ └── MainActivity.kt # 主活动界面 / Main Activity
├── application/
│ ├── SmsRepository.kt # 短信数据仓库 / SMS Repository
│ ├── SmsViewModel.kt # 短信视图模型 / SMS ViewModel
│ └── YourApplication.kt # 应用程序类 / Application Class
├── service/
│ ├── NotificationMonitorService.kt # 通知监听服务 / Notification Monitor Service
│ └── SmsHandler.kt # 短信处理器 / SMS Handler
├── receiver/
│ └── KeepAliveUtils.kt # 保活工具类 / Keep Alive Utilities
└── util/
├── DatabaseHelper.kt # 数据库助手 / Database Helper
├── LocalDatabaseHelper.kt # 本地数据库助手 / Local Database Helper
├── PermissionManager.kt # 权限管理器 / Permission Manager
├── NotificationServiceManager.kt # 通知服务管理器 / Notification Service Manager
└── KeepAliveManager.kt # 保活管理器 / Keep Alive Manager


## 功能模块说明 (Function Modules)

### 1. 核心功能 (Core Functions)
- **MainActivity** 
  - 主要功能 (Main Features)
    - 导航控制 (Navigation Control)
    - 界面管理 (UI Management)
    - 生命周期管理 (Lifecycle Management)
  
### 2. 数据管理 (Data Management)
- **数据库模块 (Database Module)**
  - LocalDatabaseHelper
    - 本地数据存储 (Local Storage)
    - 数据同步 (Data Sync)
  - DatabaseHelper
    - 远程数据库连接 (Remote Database Connection)
    - 数据操作接口 (Data Operation Interface)

### 3. 消息处理 (Message Processing)
- **短信模块 (SMS Module)**
  - SmsViewModel
    - 短信数据展示 (SMS Data Display)
    - 状态管理 (State Management)
  - SmsRepository
    - 短信数据存储 (SMS Storage)
    - 数据访问接口 (Data Access Interface)
  - SmsHandler
    - 短信处理逻辑 (SMS Processing Logic)
    - 业务规则实现 (Business Rules Implementation)

### 4. 系统服务 (System Services)
- **通知服务 (Notification Service)**
  - NotificationMonitorService
    - 通知监听 (Notification Monitoring)
    - 通知处理 (Notification Processing)
  - NotificationServiceManager
    - 服务管理 (Service Management)
    - 权限控制 (Permission Control)

### 5. 系统工具 (System Utilities)
- **权限管理 (Permission Management)**
  - PermissionManager
    - 权限检查 (Permission Check)
    - 权限请求 (Permission Request)
    - 权限结果处理 (Permission Result Handling)

- **应用保活 (Keep Alive)**
  - KeepAliveManager
    - 后台运行管理 (Background Running Management)
    - 电池优化处理 (Battery Optimization Handling)
  - KeepAliveUtils
    - 保活工具方法 (Keep Alive Utility Methods)
    - 系统设置跳转 (System Settings Navigation)

## 数据流向 (Data Flow)

用户界面 (UI) ↔ ViewModel ↔ Repository ↔ 本地/远程数据库
↕
系统服务 (Services)

## 依赖关系 (Dependencies)
- MainActivity → ViewModel
- ViewModel → Repository
- Repository → DatabaseHelper
- Service → Handler
- Manager → Utils

## 注意事项 (Notes)
1. 所有管理器类都采用单例模式
2. 数据库操作都在异步线程中执行
3. 权限请求遵循 Android 最新规范
4. 保活功能需要适配不同厂商系统

