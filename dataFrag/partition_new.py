#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
import random
import numpy as np
import json
import socket
import time
import os
from pathlib import Path

communication_address = "172.31.150.60"
communication_port = 40011

# 读取配置文件
json_file_path = './table_config.json'
with open(json_file_path, 'r', encoding='utf-8') as file:
    datas = json.load(file)

split_info = datas['split_tables']
source_table = datas['real_table_name']
have_id = datas.get('have_id', False)  # 获取是否有ID字段的配置

# 连接数据库
conn1 = pymysql.connect(
    host='172.31.150.60',
    user='root',
    port=7487,
    password='l84kCG5KP4uNNtRX',
    database='meetingData',
    cursorclass=pymysql.cursors.DictCursor
)

conn2 = pymysql.connect(
    host='172.31.150.60',
    user='root',
    port=7487,
    password='l84kCG5KP4uNNtRX',
    database='meetingData'
)

if 0:
    from sshtunnel import SSHTunnelForwarder

    server = SSHTunnelForwarder(
        ('172.28.72.225', 22),  # 指定 ssh 登录的跳转机的 address，端口号
        ssh_username='root',  # 远程服务器的用户名，注意不是 DB 的用户名和密码
        ssh_password='GDS&123softswitch',  # 远程服务器的密码
        remote_bind_address=('172.28.160.99', 13306),  # 注意端口号不要加引号
    )
    server.start()
    conn2 = pymysql.connect(host='127.0.0.1',
                            port=server.local_bind_port,
                            user='duser',
                            password='RAkj#7853',
                            database='userlogdtl',
                            connect_timeout=10,
                            )

cur1 = conn1.cursor()
cur2 = conn2.cursor()

# 获取所有数据
cur2.execute("select * from " + source_table)
results = cur2.fetchall()
description = cur2.description

results = np.array(results)
key_dict = {}
for k, kl in enumerate(description):
    key_dict[kl[0]] = k

# 创建分表
for split_table in split_info:
    table_name = split_table['split_table_name']
    columns_info = split_table['columns']

    # 删除已存在的表
    drop_query = f'DROP TABLE IF EXISTS {table_name}'
    cur1.execute(drop_query)

    # 创建新表
    if 'relation' not in split_table['split_table_name']:
        create_query = f'CREATE TABLE IF NOT EXISTS {table_name} (sid INT, '
        for k in columns_info:
            col_type = k['type'].upper()
            if col_type in ['INT', 'INTEGER', 'BIGINT']:
                create_query += f"{k['name']} {col_type}, "
            elif col_type == 'DATETIME':
                create_query += f"{k['name']} {col_type}, "
            else:
                create_query += f"{k['name']} {col_type}({k['length']}), "
    else:
        # relation表的处理
        create_query = f'CREATE TABLE IF NOT EXISTS {table_name} ('
        # 生成随机矩阵
        num_rows = results.shape[0]
        num_columns = len(columns_info) - 1  # 减去 'sid' 列

        # 生成递增的sid序列
        sid_sequence = np.arange(1, num_rows + 1)
        sid_sequence = sid_sequence.reshape(-1, 1)
        results = np.hstack([results, sid_sequence])
        key_dict['sid'] = len(key_dict.keys())
        

        # 生成其他随机ID
        for col in range(num_columns):
            unique_numbers = set()
            while len(unique_numbers) < num_rows:
                random_num = random.randint(100000000, 999999999)
                unique_numbers.add(random_num)
            unique_numbers = np.array(list(unique_numbers))
            unique_numbers = unique_numbers.reshape(-1, 1)
            results = np.hstack([results, unique_numbers])
            key_dict[f'sid{col}'] = len(key_dict.keys())

        # 添加列定义
        for k in columns_info:
            col_type = k['type'].upper()
            if col_type in ['INT', 'INTEGER', 'BIGINT']:
                create_query += f"{k['name']} {col_type}, "
            else:
                create_query += f"{k['name']} {col_type}({k.get('length', 20)}), "

    create_query = create_query[:-2] + ')'

    cur1.execute(create_query)

# 准备插入数据
target_columns = {}
for split_table in split_info:
    table_name = split_table['split_table_name']
    columns = []

    if 'relation' not in split_table['split_table_name']:
        columns.append('sid')

    for col in split_table['columns']:
        columns.append(col['name'])

    target_columns[table_name] = columns

# 构建插入语句
insert_query_dict = {}
for table_name, columns in target_columns.items():
    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    insert_query_dict[table_name] = insert_query

# 执行数据插入
i = 0
for table_name, columns in target_columns.items():
    # 根据relate_subid确定使用哪个sid
    related_sid = None
    for split_table in split_info:
        if split_table['split_table_name'] == table_name:
            related_sid = split_table.get('relate_subid')
            break

    if table_name.find('relation') != -1:
        source_keys = [key_dict['sid']]
        for col in columns:
            if col != 'sid':
                source_keys.append(key_dict[col])
    else:
        # 使用relate_subid对应的sid
        sid_number = int(related_sid.replace('sid', '')) if related_sid else i
        source_keys = [key_dict[f'sid{sid_number}']]
        for col in columns:
            if col != 'sid' and col in key_dict:
                source_keys.append(key_dict[col])
        i += 1

    insert_data = results[:, source_keys].tolist()
    insert_query = insert_query_dict[table_name]
    cur1.executemany(insert_query, insert_data)
    conn1.commit()

# 关闭数据库连接
cur1.close()
conn1.close()
cur2.close()
conn2.close()


def push_message(file_path):
    global communication_address, communication_port

    try:
        # 创建 socket 连接
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((communication_address, communication_port))
            print(f"已连接到服务器：{communication_address} 在端口：{communication_port}")

            # 读取文件内容到字节数组
            file_content = b''
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    file_content = f.read()

            # 构建 JSON 数据
            json_data = {
                "fileType": 1,
                "fileName": f"{file_path}_{int(time.time() * 1000)}",
                "fileTitle": "电信表格数据-数据更新-",
                "fileAbs": "拆分为用户信息和网页访问信息表",
                "maskIntention": "分离表格用户和网页部分的数据",
                "maskRequirements": "使用户信息分离，保护用户信息安全",
                "encryptCodeType": 1,
                "encryptLevel": 2,
                "performer_name": "system",
                "performer_code": "0215",
                "desenRequirements": ["分离表格用户和网页部分的数据"],
                "desenColumn": [],
                "maskCode": 1,
                "desenLevel": []
            }
            json_str = json.dumps(json_data)

            # 构建模拟的数据包
            buffer_size = 2 + 2 + 1 + 1 + 4 + 8 + 4 + 8 + 16 + len(json_str.encode()) + len(file_content)
            buffer = bytearray(buffer_size)

            buffer[0:2] = (2).to_bytes(2, 'big')  # 版本号
            buffer[2:4] = (1).to_bytes(2, 'big')  # 命令类别，假设为上传
            buffer[4] = 0  # 加密模式，未加密
            buffer[5] = 0  # 认证与校验模式，未签名
            buffer[6:10] = (0).to_bytes(4, 'big')  # 保留字段
            buffer[10:18] = buffer_size.to_bytes(8, 'big')  # 数据包长度
            buffer[18:22] = len(json_str.encode()).to_bytes(4, 'big')  # JSON数据包长度
            buffer[22:30] = len(file_content).to_bytes(8, 'big')  # 文件大小长度的前8字节
            buffer[30:30 + len(json_str.encode())] = json_str.encode()  # JSON数据
            buffer[30 + len(json_str.encode()):30 + len(json_str.encode()) + len(file_content)] = file_content  # 文件内容
            buffer[-16:] = b'\x00' * 16  # 认证与校验域，简化处理为全0

            # 发送数据到服务器
            s.sendall(buffer)
            print("文件数据已发送")
            time.sleep(40)  # 模拟等待40秒

            # 接收服务器的响应
            response = s.recv(1024)  # 假设响应的数据不超过1024字节
            if response:
                print(f"收到服务器的响应：{response.decode()}")
            else:
                print("服务器未返回任何数据")

    except socket.error as e:
        print(f"Socket error: {e}")
    except Exception as e:
        print(f"Error: {e}")


# 使用示例
push_message("example.xlsx")