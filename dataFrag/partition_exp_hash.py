import json
import os
import time
import subprocess
import sys

import pandas as pd

# 原始 JSON 文件列表
original_json_files = ["table_config_n2_hash.json", "table_config_n3_hash.json", "table_config_n4_hash.json"]
# 修改后的 JSON 文件路径
modified_json_file = "table_config_n_hash.json"
# 数据量配置
data_sizes = ["100w", "200w", "300w", "400w", "500w"]
# 分表脚本路径
partition_script = "partition_hash_test.py"
# 结果记录文件
partition_results = "partition_results.csv"
# 使用当前Python解释器路径
PYTHON_EXECUTABLE = sys.executable


def modify_json(original_file, data_size, suffix):
    """根据数据量修改 JSON 配置"""
    print(f"\n处理配置文件: {original_file}")
    print(f"数据量: {data_size}")
    print(f"后缀: {suffix}")

    with open(original_file, "r", encoding="utf-8") as file:
        config = json.load(file)

    config["real_table_name"] = f"meeting{data_size}"

    for split_table in config["split_tables"]:
        original_name = split_table['split_table_name']  # 例如: meeting_relation 或 meeting_server0
        table_type = original_name.split('_')[-1]  # 获取 relation 或 server0

        # 使用n2/n3/n4作为后缀，而不是hash
        new_table_name = f"meeting{data_size}_{table_type}_{suffix}_hash"
        split_table["split_table_name"] = new_table_name
        print(f"原表名: {original_name} -> 新表名: {new_table_name}")

    # 保存修改后的配置
    with open(modified_json_file, "w", encoding="utf-8") as file:
        json.dump(config, file, ensure_ascii=False, indent=4)

    print(f"\n配置文件已保存到: {modified_json_file}")


def test_partition_strategy(original_file, data_size):
    """不同数据量下不同分表策略的分表"""
    print(f"\n开始测试文件：{original_file}，数据量：{data_size}")

    # 修改这里：正确提取n2/n3/n4部分
    if "n2" in original_file:
        suffix = "n2"
    elif "n3" in original_file:
        suffix = "n3"
    elif "n4" in original_file:
        suffix = "n4"
    else:
        suffix = "unknown"

    modify_json(original_file, data_size, suffix)

    # 获取脚本的完整路径
    script_path = os.path.abspath(partition_script)
    config_path = os.path.abspath(modified_json_file)

    print(f"使用Python解释器: {PYTHON_EXECUTABLE}")
    print(f"执行脚本路径: {script_path}")
    print(f"配置文件路径: {config_path}")

    start_time = time.time()

    try:
        process = subprocess.run(
            [PYTHON_EXECUTABLE, script_path, config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True
        )

        # 打印输出
        if process.stdout:
            print("标准输出:")
            print(process.stdout)

        if process.stderr:
            print("错误输出:")
            print(process.stderr)

        partition_time = time.time() - start_time
        print(f"完成测试文件：{original_file}，数据量：{data_size}，用时：{partition_time:.2f}秒\n")
        return partition_time

    except subprocess.CalledProcessError as e:
        print(f"脚本执行失败，返回码：{e.returncode}")
        if e.stdout:
            print("标准输出:")
            print(e.stdout)
        if e.stderr:
            print("错误输出:")
            print(e.stderr)
        return None
    except Exception as e:
        print(f"执行过程中出现错误: {str(e)}")
        return None


def record_partition_time():
    """记录分表时间"""
    results = {}

    print(f"当前工作目录: {os.getcwd()}")
    print(f"脚本所在目录: {os.path.dirname(os.path.abspath(__file__))}")

    # 确保所有必要的文件都存在
    required_files = [partition_script] + original_json_files
    for file in required_files:
        if not os.path.exists(file):
            print(f"错误: 找不到所需文件 {file}")
            return

    for original_json_file in original_json_files:
        file_name = os.path.splitext(original_json_file)[0]
        for size in data_sizes:
            result = test_partition_strategy(original_json_file, size)
            if result is not None:
                results[f"{file_name}_{size}"] = result
            else:
                print(f"跳过记录失败的测试：{file_name}_{size}")

    if results:
        result_df = pd.DataFrame(
            list(results.items()),
            columns=["数据量——拆分策略", "拆分时间（秒）"]
        )

        result_df.to_csv(partition_results, index=False, encoding="utf-8-sig")
        print(f"测试完成，结果已保存至 {partition_results}")
    else:
        print("所有测试都失败，没有生成结果文件")

if __name__ == "__main__":
    # 切换到脚本所在目录
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    record_partition_time()