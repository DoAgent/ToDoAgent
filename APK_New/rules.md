登录-->是-->进入首页（登录token）
    |-->否-->注册-->用户名，邮箱，密码（自动生成user_ID）-->自动登录（登录token）

登陆后侧滑显示user_ID与用户名

local数据库
massagelocal:
+------------+--------------+------+-----+-------------------+-----------------------------+
| Field      | Type         | Null | Key | Default           | Extra                       |
+------------+--------------+------+-----+-------------------+-----------------------------+
| sender     | varchar(255) | YES  |     | NULL              |                             |
| content    | longtext     | NO   |     | NULL              |                             |
| app_name   | varchar(255) | NO   |     | NULL              |                             |
| message_id | int(13)      | NO   | PRI | NULL              |                             |
| user_id    | int(13)      | NO   | PRI | NULL              |                             |
| date       | timestamp    | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
+------------+--------------+------+-----+-------------------+-----------------------------+

todolistlocal:
+------------------+-----------------------------------------------------------+------+-----+-------------------+-----------------------------+
| Field            | Type                                                      | Null | Key | Default           | Extra                       |
+------------------+-----------------------------------------------------------+------+-----+-------------------+-----------------------------+
| todo_id          | int(13)                                                   | NO   | PRI | NULL              |                             |
| start_time       | datetime                                                  | NO   |     | CURRENT_TIMESTAMP |                             |
| end_time         | datetime                                                  | NO   |     | NULL              |                             |
| location         | varchar(255)                                              | YES  |     | NULL              |                             |
| todo_content     | mediumtext                                                | NO   |     | NULL              |                             |
| todo_statu       | enum('doing','completed','extended','stalled','Canceled') | NO   |     | doing             |                             |
| urgency_statu    | enum('unimportant','important','urgent')                  | NO   |     | unimportant       |                             |
| last_modified    | timestamp                                                 | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
+------------------+-----------------------------------------------------------+------+-----+-------------------+-----------------------------+

usertodolistlocal