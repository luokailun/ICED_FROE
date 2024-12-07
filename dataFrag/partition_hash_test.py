#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
import random
import numpy as np
import json
import socket
import time
import os
import hashlib
import sys
from pathlib import Path
from typing import Set


class HashGenerator:
    """哈希值生成器，用于生成和管理哈希值"""

    def __init__(self, min_value: int = 100000000, max_value: int = 999999999):
        self.min_value = min_value
        self.max_value = max_value
        self.used_values: Set[int] = set()

    def hash_to_int(self, value: int) -> int:
        """
        将输入值转换为指定范围内的哈希整数
        可以轻松替换为其他哈希算法，只需修改这个方法
        """
        # 使用 SHA-256 作为示例
        hash_obj = hashlib.sha256(str(value).encode())
        hash_hex = hash_obj.hexdigest()

        # 将哈希值转换为9位整数
        hash_int = int(hash_hex, 16)
        result = (hash_int % (self.max_value - self.min_value)) + self.min_value

        # 确保生成的值不重复
        while result in self.used_values:
            result = (result + 1) % (self.max_value + 1)
            if result < self.min_value:
                result = self.min_value

        self.used_values.add(result)
        return result

    def generate_initial_sid(self) -> int:
        """生成初始的随机sid"""
        while True:
            value = random.randint(self.min_value, self.max_value)
            if value not in self.used_values:
                self.used_values.add(value)
                return value


def main(config_file: str):
    """
    主函数，处理分表逻辑
    :param config_file: 配置文件路径
    """
    # 读取配置文件
    try:
        with open(config_file, 'r', encoding='utf-8') as file:
            datas = json.load(file)
    except Exception as e:
        print(f"Error reading config file {config_file}: {e}")
        sys.exit(1)

    split_info = datas['split_tables']
    source_table = datas['real_table_name']
    have_id = datas.get('have_id', False)

    print(f"Processing source table: {source_table}")

    conn1 = None
    conn2 = None
    cur1 = None
    cur2 = None

    try:
        # 连接数据库
        print("\nConnecting to database...")
        conn1 = pymysql.connect(
            host='172.31.150.60',
            user='root',
            port=7487,
            password='l84kCG5KP4uNNtRX',
            database='meetingData',  # 修改数据库名
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=30
        )

        conn2 = pymysql.connect(
            host='172.31.150.60',
            user='root',
            port=7487,
            password='l84kCG5KP4uNNtRX',
            database='meetingData',  # 修改数据库名
            charset='utf8mb4',
            connect_timeout=30
        )

        cur1 = conn1.cursor()
        cur2 = conn2.cursor()

        # 首先检查源表是否存在
        check_table_query = f"SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = 'meetingData' AND table_name = '{source_table}'"
        cur1.execute(check_table_query)
        result = cur1.fetchone()
        if result['count'] == 0:
            raise Exception(f"Source table {source_table} does not exist!")

        print(f"Source table exists: {source_table}")

        # 获取表结构
        desc_query = f"DESC {source_table}"
        cur1.execute(desc_query)
        columns_info = cur1.fetchall()
        print(f"Source table columns: {', '.join(col['Field'] for col in columns_info)}")

        # 获取所有数据
        select_query = f"SELECT * FROM {source_table}"
        print(f"\nFetching data from source table: {select_query}")
        rows_affected = cur2.execute(select_query)
        print(f"Found {rows_affected} rows in source table")

        results = cur2.fetchall()
        if not results:
            raise Exception(f"No data found in table {source_table}")

        print(f"Successfully fetched {len(results)} rows")

        description = cur2.description
        results = np.array(results)

        # 构建列索引映射
        key_dict = {}
        for k, kl in enumerate(description):
            key_dict[kl[0]] = k

        # 处理每个分表
        for split_table in split_info:
            table_name = split_table['split_table_name']
            columns_info = split_table['columns']

            print(f"\nProcessing split table: {table_name}")

            # 删除已存在的表
            drop_query = f'DROP TABLE IF EXISTS {table_name}'
            print(f"Dropping existing table if exists: {drop_query}")
            cur1.execute(drop_query)

            # 创建新表
            create_query = f'CREATE TABLE IF NOT EXISTS {table_name} (sid BIGINT, '
            for k in columns_info:
                if k['name'] != 'sid':
                    col_type = k['type'].upper()
                    if col_type in ['INT', 'INTEGER', 'BIGINT']:
                        create_query += f"{k['name']} {col_type}, "
                    elif col_type == 'DATETIME':
                        create_query += f"{k['name']} {col_type}, "
                    else:
                        create_query += f"{k['name']} {col_type}({k['length']}), "
            create_query = create_query[:-2] + ')'

            print(f"Creating new table: {create_query}")
            cur1.execute(create_query)
            conn1.commit()

        print("\nGenerating hash values...")
        # 初始化哈希生成器
        hash_gen = HashGenerator()

        # 生成所有需要的sid
        num_rows = results.shape[0]
        print(f"Generating hash values for {num_rows} rows")

        # 为每行数据生成初始sid和后续的哈希值
        initial_sids = []
        server_sids = {f"server{i}_sids": [] for i in range(4)}
        hash_ends = []

        for i in range(num_rows):
            if i > 0 and i % 10000 == 0:
                print(f"Generated hashes for {i}/{num_rows} rows")

            # 生成初始sid
            sid = hash_gen.generate_initial_sid()
            initial_sids.append(sid)

            # 生成后续的哈希值
            current_sid = sid
            for j in range(4):
                current_sid = hash_gen.hash_to_int(current_sid)
                server_sids[f"server{j}_sids"].append(current_sid)
                if j == 3:
                    hash_ends.append(current_sid)

        print("\nInserting data into split tables...")
        for split_table in split_info:
            table_name = split_table['split_table_name']
            print(f"\nPreparing data for {table_name}")

            if 'relation' in table_name:
                data = list(zip(initial_sids, hash_ends))
                columns = ['sid', 'hashEnd']
            else:
                server_num = int(table_name.split('server')[-1].split('_')[0])
                columns = ['sid'] + [col['name'] for col in split_table['columns'] if col['name'] != 'sid']
                sids = server_sids[f"server{server_num}_sids"]
                data = list(zip(sids, *[results[:, key_dict[col]] for col in columns[1:]]))

            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            print(f"Inserting {len(data)} rows into {table_name}")
            # 分批插入数据
            batch_size = 10000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cur1.executemany(insert_query, batch)
                conn1.commit()
                print(f"Inserted batch {i // batch_size + 1}/{(len(data) + batch_size - 1) // batch_size}")

        print("\nAll operations completed successfully")

    except Exception as e:
        print(f"\nError during processing: {e}")
        if conn1:
            conn1.rollback()
        raise

    finally:
        # 安全关闭数据库连接
        if cur1:
            cur1.close()
        if cur2:
            cur2.close()
        if conn1:
            conn1.close()
        if conn2:
            conn2.close()
        print("\nDatabase connections closed")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 partition_hash_test.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    if not os.path.exists(config_file):
        print(f"Config file not found: {config_file}")
        sys.exit(1)

    try:
        start_time = time.time()
        main(config_file)
        end_time = time.time()
        print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)