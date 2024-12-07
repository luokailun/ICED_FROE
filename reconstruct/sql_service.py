import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison, Where, Token, Function, Parenthesis, Values
from sqlparse.tokens import Keyword, DML, Wildcard, Operator, Literal, Punctuation
import json
import time
import pandas as pd
from collections import defaultdict
from random import *

import numpy as np
import pymysql


def build_connection():
    # 配置数据库连接参数
    # connection = pymysql.connect(
    #     host='127.0.0.1',
    #     user='root',
    #     port=3306,
    #     password='TZvTBtdd4OmdGCq',
    #     # database='dgut',
    #     database='jmt_log_stock',
    #     cursorclass = pymysql.cursors.DictCursor  # 返回结果为字典形式
    # )

    # connection = pymysql.connect(
    #     host='arm.dgut.bar',
    #     user='root',
    #     port=3306,
    #     password='fD4!pL8@nV$bX3^mZk',
    #     # database='dgut',
    #     database='jmt_log_stock',
    #     cursorclass = pymysql.cursors.DictCursor  # 返回结果为字典形式
    # )
    connection = pymysql.connect(
        host='172.31.150.60',
        user='root',
        port=7487,
        password='l84kCG5KP4uNNtRX',
        database='meetingData',
        # database='dgut',
        cursorclass=pymysql.cursors.DictCursor
    )
    # connection = pymysql.connect(
    #     host='172.28.72.225',
    #     user='root',
    #     port=7487,
    #     password='l84kCG5KP4uNNtRX',
    #     # database='dgut',
    #     database='crmdb',
    #     cursorclass=pymysql.cursors.DictCursor  # 返回结果为字典形式
    # )
    return connection


def is_subselect(parsed):
    """检查是否为子查询"""
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    """提取 FROM 和 JOIN 之后的表名"""
    from_seen = False
    join_seen = False
    for item in parsed.tokens:
        if is_subselect(item):
            yield from extract_from_part(item)
        if from_seen or join_seen:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    yield identifier.get_real_name()
            elif isinstance(item, Identifier):
                yield item.get_real_name()
            elif item.ttype is Keyword and item.value.upper() not in ('JOIN', 'ON'):
                return
        if item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
        if item.ttype is Keyword and item.value.upper() == 'JOIN':
            join_seen = True


def extract_table_names(sql):
    """从 SQL 查询中提取表名"""
    parsed = sqlparse.parse(sql)
    table_names = set()

    for stmt in parsed:
        if stmt.get_type() == 'SELECT':
            for table in extract_from_part(stmt):
                table_names.add(table)

    return list(table_names)


def extract_select_fields(parsed):
    """提取 SELECT 后的字段名"""
    select_seen = False
    for item in parsed.tokens:
        if is_subselect(item):
            yield from extract_select_fields(item)
        if select_seen:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    yield identifier.get_real_name()
            elif isinstance(item, Identifier):
                yield item.get_real_name()
            elif item.ttype is Wildcard:
                yield '*'
            elif item.ttype is Keyword and item.value.upper() == 'FROM':
                return
        if item.ttype is DML and item.value.upper() == 'SELECT':
            select_seen = True


# = {}  # 据对应的表名可以返回一个二元组，前者是一个装载select列名的数组，后者是where条件中对应的列名数组

def add_columns(stbmap, table_name, select_columns, where_columns):
    # global stbmap
    stbmap[table_name] = (select_columns, where_columns)


def init_stbmap(parse_sin, data):
    stbmap = {}
    original_table_structure_dict = dict(original_table_structure)

    ori_table_name = data['real_table_name']
    relate_table_name = data['split_tables'][0]['split_table_name']
    for split_table in data["split_tables"]:
        stbmap[f"{split_table['split_table_name']}"] = ([], [])

    first_id = True

    if (parse_sin['type'] == 'SELECT'):
        if parse_sin['fields'] == ["*"]:
            for col, key in original_table_structure:
                if "relation" in key:  # 如果是relation表
                    if first_id and data['have_id'] == True:
                        # stbmap[key][0].append(col)  # 添加 id 列
                        stbmap[key][0].append(col)  # 添加 id 列
                        first_id = False
                elif col != original_table_structure[0][1]:  # 默认输入参数的第一个是主键，如果没有也不影响
                    stbmap[key][0].append(col)
        else:
            # 处理特定列名
            # for column in select_part.split(","):
            # 假定了输入的列名全部没有表名引导，即类似R.sid这种
            # field_name = select_part.strip()  # 去掉空格
            for field_sin in parse_sin['fields']:
                stbmap[original_table_structure_dict[field_sin]][0].append(field_sin)
    elif (parse_sin['type'] == 'UPDATE'):
        pass
    elif (parse_sin['type'] == 'INSERT'):
        for table in data["split_tables"]:
            if table['split_table_name'] != data['split_tables'][0]['split_table_name']:
                stbmap[table['split_table_name']][0].append(table['relate_subid'])

        for col, tbname in original_table_structure:
            if col in parse_sin['columns']:
                stbmap[tbname][0].append(col)
            elif data['split_tables'][0]['split_table_name'] == tbname:
                parse_sin['not_included_columns'].append(col)
                stbmap[tbname][0].append(col)

        # column_list.append(f"{original_table_structure[field_name]}.{field_name}")
        # field_names.update(column_list)
        # for field in extract_select_fields(stmt):
        #     field_names.add(field)
    return stbmap


# def extract_fields(parse_sin, sql, data):   # abandon
#     """从 SQL 查询中提取字段"""
#     parsed = sqlparse.parse(sql)
#     field_names = set()
#
#     global stbmap
#     init_stbmap(data)
#
#     first_id = True
#
#     original_table_structure_dict = dict(original_table_structure)
#
#     # 目前无法处理后续存在AS别名命名的临时表中的列
#     for stmt in parsed:
#         if stmt.get_type() == 'SELECT':
#             # 提取字段名
#             for select_part in extract_select_fields(stmt):
#                 if select_part == "*":
#                     for col, key in original_table_structure:
#                         if "relation" in key: # 如果是relation表
#                             if first_id:
#                                 stbmap[key][0].append(col)  # 添加 id 列
#                                 first_id = False
#                         elif col != original_table_structure[0][1]: # 默认输入参数的第一个是主键，如果没有也不影响
#                             stbmap[key][0].append(col)
#                     break
#                 else:
#                     # 处理特定列名
#                     #for column in select_part.split(","):
#                     # 假定了输入的列名全部没有表名引导，即类似R.sid这种
#                     field_name = select_part.strip()  # 去掉空格
#                     stbmap[original_table_structure_dict[field_name]][0].append(field_name)
#                     # column_list.append(f"{original_table_structure[field_name]}.{field_name}")
#                 # field_names.update(column_list)
#                 # for field in extract_select_fields(stmt):
#                 #     field_names.add(field)
#     return stbmap


#########################
def parse_sql(sql):
    parsed_statements = sqlparse.parse(sql)
    result_list = []

    for statement in parsed_statements:
        result = parse_statement(statement)
        result_list.append(result)

    return result_list


def parse_statement(statement):
    """递归解析SQL语句"""

    for token in statement.tokens:
        # 识别SQL类型
        if token.ttype is DML:
            sql_type = token.value.upper()
            if sql_type == "SELECT":
                return parse_select(statement)
            elif sql_type == "DELETE":
                return parse_delete(statement)
            elif sql_type == "UPDATE":
                return parse_update(statement)
            elif sql_type == "INSERT":
                return parse_insert(statement)


def parse_delete(statement):
    """递归解析DELETE语句"""
    table_name = None
    where_clause = None
    for token in statement.tokens:
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            table_name = _get_next_identifier(statement.tokens, token)
        if isinstance(token, Where):
            where_clause = parse_where(token)
    return {
        'type': 'DELETE',
        'fields': [],
        'table_name': table_name,
        'where_clause': where_clause
    }


def parse_select(statement):
    """递归解析SELECT语句"""
    fields = []
    table_names = []
    where_clause = None
    limit_value = None
    offset_value = 0
    from_seen = False
    seen_From = False
    seen_Limit = False
    seen_Offset = False

    for token in statement.tokens:
        # 处理SELECT后的字段
        if token.ttype is Wildcard:
            fields.append('*')
        elif not seen_From:
            if isinstance(token, Identifier):
                fields.append(token.get_real_name())
            elif isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    fields.append(identifier.get_real_name())

        # 处理FROM后的表名
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True
            seen_From = True
            continue
        if from_seen:
            if isinstance(token, Identifier):
                table_names.append(token.get_real_name())
                from_seen = False  # 重置标志
            elif isinstance(token, IdentifierList):
                # 如果多个表名，则将它们都加入
                for identifier in token.get_identifiers():
                    table_names.append(identifier.get_real_name())
                from_seen = False  # 重置标志

        # 递归解析WHERE子句
        if isinstance(token, Where):
            where_clause = parse_where(token)

        # 解析LIMIT子句
        if token.ttype is Keyword and token.value.upper() == 'LIMIT':
            seen_Limit = True
            continue
        if seen_Limit:
            if token.ttype is Keyword and token.value.upper() == 'OFFSET':
                seen_Limit = False
                seen_Offset = True
            elif isinstance(token, IdentifierList):
                for subtoken in token:
                    if subtoken.ttype is Literal.Number.Integer:
                        if limit_value is None:
                            limit_value = int(subtoken.value)
                        else:
                            offset_value = int(subtoken.value)
                        seen_limit = False

        # 解析OFFSET子句
        if token.ttype is Keyword and token.value.upper() == 'OFFSET':
            seen_Offset = True
            continue
        if seen_Offset:
            if token.ttype is Literal.Number.Integer:
                offset_value = int(token.value)
                seen_Offset = False  # 重置标志

    return {
        'type': 'SELECT',
        'fields': fields,
        'table_name': table_names,
        'where_clause': where_clause,
        'limit_value': limit_value,
        'offset_value': offset_value
        # 'ori_where_clause': ori_where_clause
    }


def parse_update(statement):
    """递归解析UPDATE语句"""
    table_name = None
    set_clause = []
    where_clause = None
    set_seen = False

    for token in statement.tokens:
        # 处理UPDATE后的表名
        if isinstance(token, Identifier) and table_name is None:
            table_name = token.get_real_name()

        # 处理SET子句
        if token.ttype is Keyword and token.value.upper() == 'SET':
            set_seen = True
            continue

        if set_seen and isinstance(token, Comparison):
            left_side = token.left.get_real_name() if isinstance(token.left, Identifier) else str(token.left)
            right_side = str(token.right)
            set_clause.append((left_side, right_side))

        # 处理WHERE子句
        if isinstance(token, Where):
            where_clause = parse_where(token)

    return {
        'type': 'UPDATE',
        'table_name': table_name,
        'set_clause': set_clause,
        'where_clause': where_clause
    }


def parse_insert(statement):
    """递归解析INSERT语句"""
    # 解析 SQL 语句
    table_name = None
    columns = []
    values = []

    # 遍历解析的 token，查找 INSERT 关键字和 INTO
    into_seen = False
    columns_seen = False

    for token in statement:
        # 忽略 INSERT 关键字
        if token.ttype is DML and token.value.upper() == 'INSERT':
            continue
        # 检查 INTO 关键字
        if token.ttype is Keyword and token.value.upper() == 'INTO':
            into_seen = True
            continue
        # 如果 INTO 已出现，检查表名
        if into_seen:
            # 如果是 Function 类型，进一步检查
            if isinstance(token, Function):
                for sub_token in token.tokens:
                    # 解析表名
                    if isinstance(sub_token, Identifier):
                        table_name = sub_token.get_real_name()
                    # 解析列名（检查 Function 下的 Parenthesis）
                    elif isinstance(sub_token, Parenthesis) and not columns_seen:
                        # 提取列名
                        for child in sub_token.tokens:
                            if isinstance(child, IdentifierList):
                                columns = [str(id).strip() for id in child.get_identifiers()]
                        columns_seen = True
                into_seen = False  # 重置标志
                continue
        # 检查 Values 类型的 token
        if isinstance(token, Values):
            # 提取多个值部分
            for sub_token in token.tokens:
                if isinstance(sub_token, Parenthesis):
                    # 提取每个括号内的值
                    row_values = [str(val).strip() for val in sub_token.value.strip('()').split(',')]
                    values.append(row_values)
            continue

    # 返回提取的结果

    return {
        'type': 'INSERT',
        'table_name': table_name,
        'columns': columns,
        'values': values,
        'not_included_columns': []
    }


# using Operator Precedence Parse
# { -1, -1, -1, 0, -1, -2,
# -1,-1, 1, 1, -1, 1,
# -1, -1,-1, 1, -1, 1,
# -2, 1, 1, 1, -2, 1,
# -2, 1, 1, 1, -2,1,
# -1,-1, -1, -2, -1, 0};

from collections import defaultdict

f = defaultdict(lambda: 6)
g = defaultdict(lambda: 5)
f['('] = 1
f['AND'] = 4
f['OR'] = 2
f[')'] = 6
# f[default] = 6
f[';'] = 1

g['('] = 5
g['AND'] = 5
g['OR'] = 3
g[')'] = 1
# g[default] = 5
g[';'] = 1


def parse_where(where_token):
    """递归解析WHERE子句"""
    stacks = []
    stacks.append(';')
    conditions = []
    current_condition = []
    i = 0
    global f, g

    while i < len(where_token.tokens):
        token = where_token.tokens[i]
        if token.ttype is sqlparse.tokens.Whitespace or not token.value.strip() or token.value.upper() == 'WHERE':
            i += 1
            continue  # 跳过空白字符和where
        else:
            if token.value.upper() in ('(', 'AND', 'OR', ')', ';'):  # sqlparse并不能解析出左右括号
                if token.value.upper() == ';':
                    break
                elif f[stacks[-2]] <= g[token.value.upper()]:
                    stacks.append(token)
                else:
                    while isinstance(stacks[-1], list) or f[stacks[-1]] > g[token.value.upper()]:
                        if isinstance(stacks[-1], Token) and stacks[-1].value.upper() in ('(', ')'):
                            stacks.pop()
                        else:  # if stacks[-1].value.upper() in ('AND', 'OR'):
                            # if isinstance(stacks[-1], Comparison):
                            #     current_condition.append(stacks.pop())
                            #     # for sub_token in stacks[-1].tokens:
                            #     #     if isinstance(sub_token, Identifier):    #后续可以替换成检测是否为列名
                            #     #         current_condition.append([sub_token.value, stacks.pop()])
                            #     #         break
                            # else:
                            #     current_condition.append((stacks.pop()))
                            current_condition.append((stacks.pop()))
                    if len(current_condition) > 1:
                        temp = current_condition[1]
                        current_condition[1] = current_condition[0]
                        current_condition[0] = temp
                    stacks.append(current_condition)
                    current_condition = []
                    continue
            else:
                stacks.append(token)
        i += 1

    while len(stacks) > 1:
        if not isinstance(stacks[-1], list) and stacks[-1].value.upper() in ('(', ')'):
            stacks.pop()
        else:  # if stacks[-1].value.upper() in ('AND', 'OR'):
            # if isinstance(stacks[-1], Comparison):
            #     current_condition.append(stacks.pop())
            #     # for sub_token in stacks[-1].tokens:
            #     #     if isinstance(sub_token, Identifier):    #后续可以替换成检测是否为列名
            #     #         current_condition.append([sub_token.value, stacks.pop()])
            #     #         break
            # else:
            current_condition.append((stacks.pop()))
    if len(current_condition) > 1:
        temp = current_condition[1]
        current_condition[1] = current_condition[0]
        current_condition[0] = temp
        conditions = current_condition
    else:
        conditions = current_condition[0]

        # elif isinstance(token, Comparison):
        #     # left_side = token.left.get_real_name() if isinstance(token.left, Identifier) else str(token.left)
        #     # right_side = str(token.right)
        #     # operator = token.tokens[1].value  # 通常是比较运算符，如 '='
        #     stacks.push(token)
        #     # current_condition.append(token)
        #     # conditions.append(token)
        #
        #     # 处理布尔运算符（例如 "AND", "OR"）
        # elif token.ttype is Keyword and token.value.upper() in ('OR', 'AND'):
        #     # if current_condition:
        #     #     conditions.append(current_condition)  # 将当前条件加入到条件列表中
        #     #     current_condition = []  # 重置当前条件
        #     if f[stacks[-1]] > g[token.value.upper()]:
        #
        #
        # elif token.ttype is Keyword and token.value.upper() == 'WHERE':
        #     continue
        # el
        # # 处理左括号
        # elif token.ttype is Keyword and token.value == '(':
        #
        #     current_condition.append(parse_where(token))  # 递归调用
        #     continue  # 继续处理其他 tokens
        #
        # # 处理右括号
        # elif token.ttype is Keyword and token.value == ')':
        #     if current_condition:
        #         conditions.append(current_condition)  # 将当前条件加入到条件列表中
        #         current_condition = []  # 重置当前条件
        #     continue
    # 添加最后一个条件
    # if current_condition:
    #     conditions.append(current_condition)
    return conditions


def _get_next_identifier(tokens, current_token):
    """从当前token之后查找表名"""
    found = False
    for token in tokens:
        if found and isinstance(token, Identifier):
            return token.get_real_name()
        if token == current_token:
            found = True
    return None


########################


def extract_column_details(token):
    """提取列名和表别名（如果有）"""
    if isinstance(token, Identifier):
        if '.' in str(token):
            table_alias, column_name = str(token).split('.', 1)
            return table_alias.strip(), column_name.strip()
        else:
            return None, str(token).strip()
    return None, None


def split_conditions(where_clause_tokens):
    """将 WHERE 子句拆分为单独的条件，并提取列名和表别名"""
    conditions = []
    current_condition = []

    for token in where_clause_tokens:
        if not token.is_whitespace:
            if token.ttype is Operator and token.value.upper() in ('AND', 'OR'):
                if current_condition:
                    conditions.append(" ,".join(current_condition).strip())
                    current_condition = []
                conditions.append(token.value.upper())
            else:
                current_condition.append(str(token).strip())

    if current_condition:
        conditions.append(" ,".join(current_condition).strip())

    return conditions


# def extract_where_conditions(parsed):
#     """提取 WHERE 子句，并将其拆分为单独的条件"""
#     conditions = []
#     columns = []
#     # 目前问题：抽取列和条件的过程中破坏了其原有语法树的相对位置关系，抽取出的两个条件不知道是什么关系
#     # 此处需要修改： 对原有条件子句进行优化，确保语法树上满足以下条件，1.同表的查询尽量在一个子句中
#     # 2.如果连续的and或or所连接的条件不在同一个子表中，需要拆成多级条件
#     # 原句：a.con1 and a.con2 and a.con3 or b.con4 and b.con5 and a.con6
#     # - T1
#     #   - a.con1
#     #   - and
#     #   - a.con2
#     #   - and
#     #   - a.con3
#     # - or
#     # - T2
#     #   - T3
#     #     - b.con4
#     #     - and
#     #     - b.con5
#     #   - and
#     #   - a.con6
#
#
#     for item in parsed.tokens:
#         if isinstance(item, Where):
#             where_tokens = [token for token in item.tokens if token.ttype is not Keyword or token.value.upper() != "WHERE"]
#             conditions = split_conditions(where_tokens)
#
#             for token in where_tokens:
#                 if isinstance(token, Comparison):
#                     for sub_token in token.tokens:
#                         if isinstance(sub_token, Identifier):
#                             table_alias, column_name = extract_column_details(sub_token)
#                             if column_name:
#                                 columns.append((table_alias, column_name, token))  # 需要确定条件之间的关系
#
#     return conditions, columns


class ColumnMapping:
    def __init__(self, column_name, table_name):
        self.column_name = column_name
        self.table_name = table_name


original_table_structure = []
columnsMapsTab = defaultdict(str)
relatesidx = defaultdict(str)
idName = ""


def read_config(json_file):
    global original_table_structure, columnsMapsTab, idName
    original_table_structure = []

    try:
        with open(json_file, 'r') as input_stream:
            data = json.load(input_stream)
            idName = data['split_tables'][0]['columns'][0]['name']

            ori_table_name = data['real_table_name']

            for split_table in data["split_tables"]:
                split_table_name = f"{split_table['split_table_name']}"
                columns = split_table["columns"]
                relatesidx[split_table_name.lower()] = split_table['relate_subid']
                for column in columns:
                    column_name = column.get("name")
                    original_table_structure.append((column_name.lower(), split_table_name.lower()))  # 使用字典映射
                    columnsMapsTab[column_name.lower()] = split_table_name.lower()
            return data
    except IOError as e:
        print(f"文件读取错误: {e}")
    except Exception as e:
        print(e)

    except IOError as e:
        print(f"文件读取错误: {e}")
    except Exception as e:
        print(e)


# def extract_conditions(stbmap, sql, bsubop = True):
#     """从SQL查询中提取并拆分 WHERE 条件，以及提取表别名和列名"""
#     parsed = sqlparse.parse(sql)
#     conditions = []
#     columns = []
#
#     for stmt in parsed:
#         if stmt.get_type() == 'SELECT':
#             where_conditions, where_columns = extract_where_conditions(stmt) # 此处应该直接让
#             if where_conditions:
#                 conditions.extend(where_conditions)
#             if where_columns:
#                 columns.extend(where_columns)
#
#     if not bsubop:
#         original_table_structure_dict = dict(original_table_structure)
#         for column in columns:
#             stbmap[original_table_structure_dict[column[1]]][1].append(column)
#
#     return conditions, columns

def ext_where2stbmap(prase_sin, stbmap):
    """从SQL查询中提取并拆分 WHERE 条件，以及提取表别名和列名"""

    columns = []

    for token in prase_sin['where_clause']:
        if isinstance(token, Comparison):
            for sub_token in token.tokens:
                if isinstance(sub_token, Identifier):
                    table_alias, column_name = extract_column_details(sub_token)
                    if column_name:
                        columns.append((table_alias, column_name, token))  # 需要确定条件之间的关系

        elif isinstance(token, Keyword):
            pass

    original_table_structure_dict = dict(original_table_structure)
    for column in columns:
        stbmap[original_table_structure_dict[column[1]]][1].append(column)


def parse_column_details_on(token):
    """提取 ON 子句中的列名和表别名（如果有）"""
    if isinstance(token, Identifier):
        if '.' in str(token):
            table_alias, column_name = str(token).split('.', 1)
            return table_alias.strip(), column_name.strip()
        else:
            return None, str(token).strip()
    return None, None


def extract_on_clause_conditions(parsed):
    """提取 ON 子句中的条件和表别名、列名"""
    conditions = []
    columns = []
    on_clause_detected = False

    for token in parsed.tokens:
        if token.ttype is Keyword and token.value.upper() == 'ON':
            on_clause_detected = True
            continue

        if on_clause_detected:
            if isinstance(token, Comparison):
                conditions.append(str(token).strip())
                for sub_token in token.tokens:
                    if isinstance(sub_token, Identifier):
                        table_alias, column_name = parse_column_details_on(sub_token)
                        if column_name:
                            columns.append((table_alias, column_name))

    return conditions, columns


def extract_on_conditions_from_sql(sql):
    """从SQL查询中提取 ON 子句条件，并获取表别名和列名"""
    parsed = sqlparse.parse(sql)
    on_clause_conditions = []
    on_clause_columns = []

    for stmt in parsed:
        if stmt.get_type() == 'SELECT':
            conditions, columns = extract_on_clause_conditions(stmt)
            if conditions:
                on_clause_conditions.extend(conditions)
            if columns:
                on_clause_columns.extend(columns)

    return on_clause_conditions, on_clause_columns


def query_sid(querysql, data):
    parse_ans = parse_sql(querysql)

    # global table_name, prefix, rlationtabname
    # table_name = data['real_table_name']  # 可考虑是否优化配置文件
    # prefix = data['split_table_prefix']
    # rlationtabname = table_name + '_relation'

    # r = get_relation(table_name + '_relation')   #这里相当于调用了一次sql全表查询，如果数据量较大的时候存在性能问题
    # print(r)
    for parse_single in parse_ans:
        # 生成子查询
        # sub_queries = generate_sub_queries(querysql, data)  #即使限定了只有两个表，但限定了条件从属表的顺序，以及必须来自于前后两个不同的表
        sub_sqls, stbmap = generate_sub_sql_fromwhere(parse_single, data)

        # r = get_relation(data['split_table_names'])  # 这里相当于调用了一次sql全表查询，如果数据量较大的时候存在性能问题
        if parse_single['type'] == 'SELECT':
            query_results = []
            for query in sub_sqls:
                res = execute_sql_query(query[2])  # 每次执行sql的时候都会重建连接
                query_results.append((query[0], query[1], res))

            if len(query_results) == 1:
                # 林老师的代码
                df_sidx = single_condition(query_results[0])
                # 测试用
                # cur_result=single_condition(query_results[0],r,'sada_gdpi_click_dtl_server1')
            else:
                operator = parse_single['where_clause'][1]  # 应对大小写不敏感(已处理)

                # if len(operators)+1 != len(query_results):
                #     print("error, operator extractor has problem")

                # for i in range(len(operators)): # 只能用于简单的纯and/纯or子句，否则不行
                # if i==0:
                cur_result = query_results[0]
                if operator.value in "AND":
                    tar_result = query_results[1]
                    df_sidx = and_operation(cur_result, tar_result)
                if operator.value in "OR":
                    tar_result = query_results[1]
                    df_sidx = or_operation(cur_result, tar_result)  # 使用了分条查询，性能瓶颈

            cur_result = exe_compose_query(stbmap, df_sidx, data)
    return cur_result


def rec_gen_sqltree(tree):
    global columnsMapsTab, relatesidx
    sql = ''
    if isinstance(tree, list):
        if len(tree) == 3:
            i = 1
            while i < 3:
                if isinstance(tree[i], list):
                    tree[i] = rec_gen_sqltree(tree[i])
                else:
                    for sub_token in tree[i]:
                        if isinstance(sub_token, Identifier):
                            # tree[i] = (f"SELECT sid FROM {columnsMapsTab[sub_token.value]} WHERE {tree[i].value}")
                            tree[i] = (relatesidx[columnsMapsTab[sub_token.value.lower()]],
                                       columnsMapsTab[relatesidx[columnsMapsTab[sub_token.value.lower()]]],
                                       f"SELECT sid FROM {columnsMapsTab[sub_token.value.lower()]} WHERE {tree[i].value}")
                            break
                    # = sql
                i += 1
        else:
            for sub_token in tree[0]:
                if isinstance(sub_token, Identifier):
                    # tree[i] = (f"SELECT sid FROM {columnsMapsTab[sub_token.value]} WHERE {tree[i].value}")
                    # if relatesidx[columnsMapsTab[sub_token.value.upper()]] == idName:
                    #     sql = f"SELECT {idName} FROM {columnsMapsTab[sub_token.value.upper()]} WHERE {tree[0].value}"
                    # else:
                    #     sql = f"SELECT sid FROM {columnsMapsTab[sub_token.value.upper()]} WHERE {tree.value}"
                    if relatesidx[columnsMapsTab[sub_token.value.lower()]] == idName:
                        sql = f"SELECT {idName} FROM {columnsMapsTab[sub_token.value.lower()]} WHERE {tree[0].value}"
                    else:
                        sql = f"SELECT sid FROM {columnsMapsTab[sub_token.value.lower()]} WHERE {tree[0].value}"
                    tree = (relatesidx[columnsMapsTab[sub_token.value.lower()]],
                            columnsMapsTab[relatesidx[columnsMapsTab[sub_token.value.lower()]]],
                            sql)
                    # tree = (relatesidx[columnsMapsTab[sub_token.value.upper()]],
                    #         columnsMapsTab[relatesidx[columnsMapsTab[sub_token.value.upper()]]],
                    #         sql)
                    break
        return tree
    else:
        for sub_token in tree:
            if isinstance(sub_token, Identifier):
                # tree[i] = (f"SELECT sid FROM {columnsMapsTab[sub_token.value]} WHERE {tree[i].value}")
                if relatesidx[columnsMapsTab[sub_token.value.lower()]] == idName:
                    sql = f"SELECT {idName} FROM {columnsMapsTab[sub_token.value.lower()]} WHERE {tree.value}"
                else:
                    sql = f"SELECT sid FROM {columnsMapsTab[sub_token.value.lower()]} WHERE {tree.value}"
                tree = (relatesidx[columnsMapsTab[sub_token.value.lower()]],
                        columnsMapsTab[relatesidx[columnsMapsTab[sub_token.value.lower()]]],
                        sql)
                return tree


def generate_sub_sql_fromwhere(parse_sin, data):
    """根据查询和 JSON 文件生成子查询"""
    # 目前问题：两个条件可能来自同一个子表，这个时候就没必要生成两个子查询
    # 应该根据查询的条件与所查询列所属的子表来分类
    # 在提取出where和select之后，一个子查询一个循环即可
    # field_names = extract_fields(parse_sin, test_query, data)
    stbmap = init_stbmap(parse_sin, data)

    tree = parse_sin['where_clause']
    if tree == None:
        tree = ("sid0", data['split_tables'][0]['split_table_name'], f"select sid from {data['split_tables'][1]['split_table_name']}")
    else:
        tree = rec_gen_sqltree(tree)
    return tree, stbmap

    # 解析 SQL 查询
    # conditions_1, columns_1 = extract_conditions(test_query, False)
    # ext_where2stbmap(parse_sin, stbmap)

    # 从JSON文件中读取数据
    # with open(json_file_path, 'r') as file:
    #     data = json.load(file)

    # global table_name, prefix, rlationtabname
    # table_name = data['real_table_name']    #可考虑是否优化配置文件
    # ori_table_name = data['split_table_prefix']
    # rlationtabname = data['split_tables'][0]['split_table_name']
    # # ori_table_name = data['table-config']['tables'][0]['real_table_name']
    # # prefix = data['table-config']['tables'][2]['split_table_prefix']
    # id_name = data['split_tables'][0]['columns'][0]['name']
    # sub_query_sqls = []
    # if parse_sin['type'] == 'SELECT':
    #     for table in data['split_tables']:
    #         if 'relation' not in table['split_table_name']:
    #             stbname = table['split_table_name']
    #             # sub_query_fields = ",".join(field_names[stbname][0])
    #             for con in stbmap[stbname][1]:
    #                 #sql = f"SELECT sid, {sub_query_fields} FROM {stbname} WHERE {con[2]}"
    #                 sql = f"SELECT sid FROM {stbname} WHERE {con[2]}"
    #                 sub_query_sqls.append((stbname, table['split_table_name'].replace(prefix, "sid"), sql))
    #                 # sql = f"SELECT sid FROM {stbname} WHERE sid IN ({sub_query_fields})"
    #                 # postsqls.append((stbname, table['split_table_name'].replace(prefix, "sid"),sql))
    # elif parse_sin['type'] == 'DELETE': # physical
    #     sql = f"SELECT {id_name} FROM {ori_table_name} {parse_sin['ori_where_clause']}"
    #     df = query_sid(sql,data)
    #     print(df)
    # sqls.append((stbname, table['split_table_name'].replace(prefix, "sid"), sql, con[2]))

    # 遍历 data['tables']
    # for table in data['table-config']['tables'][1]['split_tables']:
    #     tar_fields = []
    #     table_field_map = []
    #     col_list = []
    #     for column in table['columns']:
    #         col_list.append(column['name'])
    #     for tar_col in extract_fields(test_query):
    #         if tar_col in col_list:
    #             tar_fields.append(tar_col)
    #     table_field_map = ({f"{ori_table_name}_{table['split_table_name']}": tar_fields})
    #             # break
    #
    #     for col in columns_1:
    #         column_name = col[1]
    #         tar_conditions = []
    #         if column_name in col_list:
    #             conditions = conditions_1[0].split(' ,')
    #             for condition in conditions:
    #                 if column_name in condition:
    #                     tar_conditions.append(condition)
    #                     sub_query_fields = ",".join(table_field_map[f"{ori_table_name}_{table['split_table_name']}"])
    #                     sql = f"SELECT sid, {sub_query_fields} FROM {ori_table_name}_{table['split_table_name']} WHERE {condition}"
    #                     sqls.append(sql)

    # return sub_query_sqls, stbmap


# def regen_sub_queries(test_query, data):
#     """根据查询和 JSON 文件生成子查询"""
#     # 目前问题：两个条件可能来自同一个子表，这个时候就没必要生成两个子查询
#     # 应该根据查询的条件与所查询列所属的子表来分类
#     # 在提取出where和select之后，一个子查询一个循环即可
#     field_names = extract_fields(test_query, data)
#
#     # 解析 SQL 查询
#     conditions_1, columns_1 = extract_conditions(test_query, False)
#
#     # 从JSON文件中读取数据
#     # with open(json_file_path, 'r') as file:
#     #     data = json.load(file)
#
#     ori_table_name = data['table-config']['tables'][0]['real_table_name']
#     prefix = data['table-config']['tables'][2]['split_table_prefix']
#     sqls = []
#
#     for table in data['table-config']['tables'][1]['split_tables']:
#         if 'relation' not in table['split_table_name']:
#             stbname = f"{ori_table_name}_{table['split_table_name']}"
#             sub_query_fields = ",".join(field_names[stbname][0])
#             for con in field_names[stbname][1]:
#                 # sql = f"SELECT sid, {sub_query_fields} FROM {stbname} WHERE {con[2]}"
#                 sql = f"SELECT sid FROM {stbname} WHERE {con[2]}"
#                 sqls.append((stbname, table['split_table_name'].replace(prefix, "sid"), sql))
#
#     # 遍历 data['tables']
#     # for table in data['table-config']['tables'][1]['split_tables']:
#     #     tar_fields = []
#     #     table_field_map = []
#     #     col_list = []
#     #     for column in table['columns']:
#     #         col_list.append(column['name'])
#     #     for tar_col in extract_fields(test_query):
#     #         if tar_col in col_list:
#     #             tar_fields.append(tar_col)
#     #     table_field_map = ({f"{ori_table_name}_{table['split_table_name']}": tar_fields})
#     #             # break
#     #
#     #     for col in columns_1:
#     #         column_name = col[1]
#     #         tar_conditions = []
#     #         if column_name in col_list:
#     #             conditions = conditions_1[0].split(' ,')
#     #             for condition in conditions:
#     #                 if column_name in condition:
#     #                     tar_conditions.append(condition)
#     #                     sub_query_fields = ",".join(table_field_map[f"{ori_table_name}_{table['split_table_name']}"])
#     #                     sql = f"SELECT sid, {sub_query_fields} FROM {ori_table_name}_{table['split_table_name']} WHERE {condition}"
#     #                     sqls.append(sql)
#
#     return sqls

# def generate_sub_ids(test_query, json_file_path):
#     """根据查询和 JSON 文件生成子查询"""
#
#     # 解析 SQL 查询
#     conditions_1, columns_1 = extract_conditions(test_query)
#
#     # 从JSON文件中读取数据
#     with open(json_file_path, 'r') as file:
#         data = json.load(file)
#
#     sqls = []
#
#     # 遍历 data['tables']
#     for table in data['tables']:
#         tar_fields = []
#         for tar_col in extract_fields(test_query):
#             if tar_col in data[table]:
#                 tar_fields.append(tar_col)
#                 table_field_map = {table: tar_fields}
#
#         for col in columns_1:
#             column_name = col[1]
#             tar_conditions = []
#             if column_name in data[table]:
#                 conditions = conditions_1[0].split(' ,')
#                 for condition in conditions:
#                     if column_name in condition:
#                         tar_conditions.append(condition)
#                         sub_query_fields = ",".join(table_field_map[table])
#                         sql = f"SELECT sid FROM {table} WHERE {condition}"
#                         sqls.append(sql)
#
#     return sqls

def exe_compose_delete(stbmap, df_sids, data, bool_df=True, temp_table_name="temptable"):
    results = []
    conn = build_connection()
    # if len(df_sids) == 0:
    #     return connect_result(stbmap, df_sids, data, results, bool_df)
    try:
        # 创建游标对象
        with conn.cursor() as cur:
            # 执行 SQL 查询

            r_list_str = ', '.join(map(str, df_sids[idName].tolist()))
            if r_list_str != 'None':
                sql = f"SELECT * FROM {data['split_tables'][0]['split_table_name']} WHERE {data['split_tables'][0]['columns'][0]['name']} IN ({r_list_str})"
            else:
                return None
            
            result = AvoidNullResult(cur, sql)
            # cur.execute(sql)
            # res = cur.fetchall()
            df_sidx = pd.DataFrame(result)

            create_table_query = f"""
            CREATE TEMPORARY TABLE {temp_table_name} ("""
            for colum in data['split_tables'][0]["columns"]:
                create_table_query += colum['name'] + """ VARCHAR(20), """
            create_table_query = create_table_query[:-2] + ")"
            # create_table_query += """)
            # """

            cur.execute(create_table_query)

            ori_table_name = data['real_table_name']
            # prefix = data['split_table_prefix']

            # 插入 DataFrame 中的数据到 MySQL 临时表中
            insert_query = f"INSERT INTO {temp_table_name} ({','.join(df_sidx.columns)}) VALUES ({','.join(['%s'] * len(df_sidx.columns))})"
            cur.executemany(insert_query, df_sidx.values.tolist())

            for table in data['split_tables']:
                stbname = table['split_table_name']
                if stbname != data['split_tables'][0]['split_table_name']:
                    sql = f"Delete FROM {stbname} WHERE sid IN (SELECT {table['relate_subid']} FROM temptable)"
                else:
                    sql = f"Delete FROM {stbname} WHERE {data['split_tables'][0]['columns'][0]['name']} IN (SELECT {data['split_tables'][0]['columns'][0]['name']} FROM temptable)"
                
                cur.execute(sql)
                conn.commit()

                #
                # # 获取查询结果
                # result = cur.fetchall()
                #
                # # 获取列名
                # column_names = [desc[0] for desc in cur.description]
                #
                #
                # # 检查结果集是否为空
                # if not result:
                #     # 创建一个空的字典列表，其中包含所有列名作为键
                #     result = [{column: None for column in column_names}]

                # results.append((result, table['relate_subid']))

                # else:
                #     sql = f"SELECT sid FROM {stbname} WHERE sid IN (SELECT {table['relate_subid']} FROM temptable)"

            return None
    except Exception as e:
        print(f"exe_compose_query SQL 执行错误: {e}")
        return None
    finally:
        # 关闭数据库连接
        conn.close()


# def generate_sub_operator(test_query):
#     con, col = extract_conditions(test_query)
#     result = con[0].split(",")
#     oprs = []
#     for c in result:
#         c = c.strip().upper()  # 转换为大写
#         if "AND" in c:
#             oprs.append("AND")
#         if "OR" in c:
#             oprs.append("OR")
#     return oprs

def AvoidNullResult(cur, sql):
    # 
    cur.execute(sql)

    # 获取查询结果
    result = cur.fetchall()

    # 获取列名
    column_names = [desc[0] for desc in cur.description]

    # 检查结果集是否为空
    if not result:
        # 创建一个空的字典列表，其中包含所有列名作为键
        result = [{column: None for column in column_names}]
    return result


def exe_compose_query(stbmap, df_sids, data, bool_df=True, temp_table_name="temptable"):
    results = []
    conn = build_connection()
    # if len(df_sids) == 0:
    #     return connect_result(stbmap, df_sids, data, results, bool_df)
    try:
        # 创建游标对象
        with conn.cursor() as cur:
            # 执行 SQL 查询

            r_list_str = ', '.join(map(str, df_sids[idName].tolist()))
            if r_list_str != 'None':
                sql = f"SELECT * FROM {data['split_tables'][0]['split_table_name']} WHERE {data['split_tables'][0]['columns'][0]['name']} IN ({r_list_str})"
            else:
                sql = f"SELECT * FROM {data['split_tables'][0]['split_table_name']} WHERE {data['split_tables'][0]['columns'][0]['name']} = -1"
            
            result = AvoidNullResult(cur, sql)
            # cur.execute(sql)
            # res = cur.fetchall()
            df_sidx = pd.DataFrame(result)

            create_table_query = f"""
            CREATE TEMPORARY TABLE {temp_table_name} ("""
            for colum in data['split_tables'][0]["columns"]:
                create_table_query += colum['name'] + """ VARCHAR(20), """
            create_table_query = create_table_query[:-2] + ")"
            # create_table_query += """)
            # """

            cur.execute(create_table_query)

            ori_table_name = data['real_table_name']
            # prefix = data['split_table_prefix']

            # 插入 DataFrame 中的数据到 MySQL 临时表中
            insert_query = f"INSERT INTO {temp_table_name} ({','.join(df_sidx.columns)}) VALUES ({','.join(['%s'] * len(df_sidx.columns))})"
            cur.executemany(insert_query, df_sidx.values.tolist())

            for table in data['split_tables']:
                if table['split_table_name'] != data['split_tables'][0]['split_table_name']:
                    # sql = f"SELECT sid FROM {table['split_table_name']}"
                    stbname = table['split_table_name']
                    sub_query_fields = ",".join(stbmap[stbname][0])
                    if len(stbmap[stbname][0]) > 0:
                        if r_list_str != 'None':
                            sql = f"SELECT sid, {sub_query_fields} FROM {stbname} WHERE sid IN (SELECT {table['relate_subid']} FROM temptable)"
                        else:
                            sql = f"SELECT sid, {sub_query_fields} FROM {stbname} WHERE sid = -1"
                        
                        result = AvoidNullResult(cur, sql)
                        # cur.execute(sql)
                        #
                        # # 获取查询结果
                        # result = cur.fetchall()
                        #
                        # # 获取列名
                        # column_names = [desc[0] for desc in cur.description]
                        #
                        #
                        # # 检查结果集是否为空
                        # if not result:
                        #     # 创建一个空的字典列表，其中包含所有列名作为键
                        #     result = [{column: None for column in column_names}]

                        results.append((result, table['relate_subid']))

                    # else:
                    #     sql = f"SELECT sid FROM {stbname} WHERE sid IN (SELECT {table['relate_subid']} FROM temptable)"

            return connect_result(stbmap, df_sidx, data, results, bool_df)
    except Exception as e:
        print(f"exe_compose_query SQL 执行错误: {e}")
        return None
    finally:
        # 关闭数据库连接
        conn.close()

def exe_fulltable_query(parse_single, stbmap, data, bool_df=True):
    results = []
    conn = build_connection()
    # if len(df_sids) == 0:
    #     return connect_result(stbmap, df_sids, data, results, bool_df)
    try:
        # 创建游标对象
        with conn.cursor() as cur:
            # 执行 SQL 查询


            fulltable_sql = f'select * from {data["split_tables"][0]["split_table_name"]} limit 1000'
            print(f'\texecute\tselect * from {data["split_tables"][0]["split_table_name"]}')
            cur.execute(fulltable_sql)
            df_sidx = pd.DataFrame(cur.fetchall())

            ori_table_name = data['real_table_name']

            for table in data['split_tables']:
                if table['split_table_name'] != data['split_tables'][0]['split_table_name']:
                    # sql = f"SELECT sid FROM {table['split_table_name']}"
                    stbname = table['split_table_name']
                    sub_query_fields = ",".join(stbmap[stbname][0])
                    if len(stbmap[stbname][0]) > 0:
                        sql = f"SELECT * FROM {stbname}  limit 1000"
                        # sql = f"SELECT sid, {sub_query_fields} FROM {stbname} "
                        print(f'\texecute\tSELECT * FROM {stbname}')
                        result = AvoidNullResult(cur, sql)
                        # cur.execute(sql)
                        #
                        # # 获取查询结果
                        # result = cur.fetchall()
                        #
                        # # 获取列名
                        # column_names = [desc[0] for desc in cur.description]
                        #
                        #
                        # # 检查结果集是否为空
                        # if not result:
                        #     # 创建一个空的字典列表，其中包含所有列名作为键
                        #     result = [{column: None for column in column_names}]

                        results.append((result, table['relate_subid']))

                    # else:
                    #     sql = f"SELECT sid FROM {stbname} WHERE sid IN (SELECT {table['relate_subid']} FROM temptable)"
            print('connecting results')
            return connect_result(stbmap, df_sidx, data, results, bool_df)
    except Exception as e:
        print(f"exe_compose_query SQL 执行错误: {e}")
        return None
    finally:
        # 关闭数据库连接
        conn.close()


def connect_result(stbmap, df_sidx, data, results, bool_df=True):
    result = []  # python中，使用多层for循环

    for index, result in enumerate(results):
        df = pd.DataFrame(result[0])
        if index == 0:
            merged_df = pd.merge(df_sidx, df, left_on=result[1], right_on='sid', how="inner", suffixes=('', '_dump'))
        else:
            merged_df = pd.merge(merged_df, df, left_on=result[1], right_on='sid', how="inner", suffixes=('', '_dump'))

    regstr = '^(?!.*_dump)(?!sid[0-9]+)'
    if columnsMapsTab['sid'] == '':
        regstr += '(?!sid)'
    merged_df = merged_df.filter(regex=regstr)
    if bool_df:
        return merged_df
    else:
        # 定义 to_camel_case 函数
        def to_camel_case(field_name):
            parts = field_name.split('_')
            if parts:
                parts[0] = parts[0].lower()
            for i in range(1, len(parts)):
                parts[i] = parts[i].capitalize()
            return ''.join(parts)

        # 动态生成新列名
        new_columns = [to_camel_case(col) for col in merged_df.columns]
        merged_df.columns = new_columns

        def convert_datetime_to_mysql_format(df):
            # 遍历DataFrame的所有列
            for col in df.columns:
                # 检查列的数据类型是否为datetime64[ns]
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    # 将datetime列格式化为MySQL的DATETIME格式
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            return df

        merged_df = convert_datetime_to_mysql_format(merged_df)
        return merged_df.to_dict(orient='records')


def execute_sql_query(sql_query):
    connection = build_connection()

    try:
        # 创建游标对象
        with connection.cursor() as cursor:
            # 执行 SQL 查询
            cursor.execute(sql_query)

            # 获取所有查询结果
            result = cursor.fetchall()
            return result
    except Exception as e:
        print(f"execute_sql_query SQL 执行错误: {e}")
        return None
    finally:
        # 关闭数据库连接
        connection.close()


def rec_exeSqlTree(sqlTree, cur):
    try:
        if isinstance(sqlTree, list):
            if len(sqlTree) == 3:
                i = 1
                while i < len(sqlTree):
                    if isinstance(sqlTree[i], tuple):
                        # cur.execute(sqlTree[i][2])
                        print(f'\texecute\t{sqlTree[i][2]}')
                        res = AvoidNullResult(cur, sqlTree[i][2])
                        sqlTree[i] = (sqlTree[i][0], sqlTree[i][1], pd.DataFrame(res))
                    else:
                        sqlTree[i] = rec_exeSqlTree(sqlTree[i], cur)
                    i += 1
                return sqlTree
            else:
                # cur.execute(sqlTree[0][2])
                print(f'\texecute\t{sqlTree[0][2]}')
                res = AvoidNullResult(cur, sqlTree[0][2])
                # sqlTree[0] = (sqlTree[0][0], sqlTree[0][1], pd.DataFrame(cur.fetchall()))
                return (sqlTree[0][0], sqlTree[0][1], pd.DataFrame(res))
        elif isinstance(sqlTree, tuple):
            print(f'\texecute\t{sqlTree[2]}')
            res = AvoidNullResult(cur, sqlTree[2])
            # sqlTree[0] = (sqlTree[0][0], sqlTree[0][1], pd.DataFrame(cur.fetchall()))
            return (sqlTree[0], sqlTree[1], pd.DataFrame(res))
    except Exception as e:
        print(f"rec_exeSqlTree SQL 执行错误: {e}")


def exeSqlTree(sqlTree):
    restree = sqlTree
    connection = build_connection()

    try:
        # 创建游标对象
        cur = connection.cursor()
        restree = rec_exeSqlTree(restree, cur)
        return restree
    except Exception as e:
        print(f"exeSqlTree SQL 执行错误: {e}")
        return None
    finally:
        # 关闭数据库连接
        connection.close()


def rec_procRes2Sid(ResTree, cur):
    try:
        if isinstance(ResTree, list):
            if len(ResTree) == 3:
                i = 1
                while i < len(ResTree):
                    if isinstance(ResTree[i], tuple):
                        # cur.execute(ResTree[i][2])
                        # ResTree[i] = (ResTree[i][0], ResTree[i][1], pd.DataFrame(cur.fetchall()))

                        sid_list_str = ', '.join(map(str, ResTree[i][2]['sid'].tolist()))
                        if sid_list_str != 'None':
                            sql = f"SELECT sid FROM {ResTree[i][1]} WHERE {ResTree[i][0]} IN ({sid_list_str})"
                        else:
                            sql = f"SELECT sid FROM {ResTree[i][1]} WHERE {ResTree[i][0]} = -1"
                        # cur.execute(sql)
                        
                        res = AvoidNullResult(cur, sql)
                        ResTree[i] = pd.DataFrame(res)

                    else:
                        rec_procRes2Sid(ResTree[i], cur)
                    i += 1
            else:
                sid_list_str = ', '.join(map(str, ResTree[0][2]['sid'].tolist()))
                if sid_list_str != 'None':
                    sql = f"SELECT sid FROM {ResTree[0][1]} WHERE {ResTree[0][0]} IN ({sid_list_str})"
                else:
                    sql = f"SELECT sid FROM {ResTree[0][1]} WHERE {ResTree[0][0]} = -1"
                
                res = AvoidNullResult(cur, sql)
                ResTree[0] = pd.DataFrame(res)

        elif isinstance(ResTree, tuple):
            if ResTree[0] != idName:
                sid_list_str = ', '.join(map(str, ResTree[2]['sid'].tolist()))
                if sid_list_str != 'None':
                    sql = f"SELECT {idName} FROM {ResTree[1]} WHERE {ResTree[0]} IN ({sid_list_str})"
                else:
                    sql = f"SELECT {idName} FROM {ResTree[1]} WHERE {ResTree[0]} = -1"
                # cur.execute(sql)
                
                res = AvoidNullResult(cur, sql)
                ResTree = pd.DataFrame(res)
            else:
                ResTree = pd.DataFrame(ResTree[2])
        return ResTree
    except Exception as e:
        print(f"rec_procRes2Sid SQL 执行错误: {e}")


def procRes2Sid(ResTree):
    newrestree = ResTree
    connection = build_connection()

    try:
        # 创建游标对象
        cur = connection.cursor()
        newrestree = rec_procRes2Sid(newrestree, cur)
        return newrestree
    except Exception as e:
        print(f"procRes2Sid SQL 执行错误: {e}")
        return None
    finally:
        # 关闭数据库连接
        connection.close()


def get_relation(relation_name):
    query = "select * from " + relation_name + ";"
    relations = execute_sql_query(query)
    relation = pd.DataFrame(relations)

    return relation


# def and_operation_bak(result_1,result_2,r):
#     result = []  # python中，使用多层for循环
#     for i in range(len(result_1)):
#         for j in range(len(r)):
#             if result_1[i]['sid']==r[j]['sid0']:
#                 # ids.append(r[j]['sid1'])
#                 for k in range(len(result_2)):
#                     # print('====here====')
#                     if r[j]['sid1']==result_2[k]['sid']:
#                         merged_result = result_1[i] | result_2[k]
#                         result.append(merged_result)

#     print(len(result))
#     return result

# 需要优化，需要知道传入数据对应的表对应在relation中sid_num
# 现在的sid_num是写死的
def and_operation(result_1, result_2, r):
    # result = []  # python中，使用多层for循环
    df1 = pd.DataFrame(result_1[2])
    merged_df = pd.merge(r, df1, left_on=result_1[1], right_on='sid', how="inner", suffixes=('', '_dump'))

    df2 = pd.DataFrame(result_2[2])
    merged_df = pd.merge(merged_df, df2, left_on=result_2[1], right_on='sid', how="inner", suffixes=('', '_dump'))
    return merged_df.filter(regex='^(?!.*_dump)')


def and_op(df_res_1, df_res_2):
    # sids1 = set(df_res_1['sid'])
    # sids2 = set(df_res_2['sid'])
    #
    # common_sids = sids1 & sids2
    #
    # # 将结果转换回 DataFrame
    # result_df = pd.DataFrame(list(common_sids), columns=['sid'])
    merged_df = pd.merge(df_res_1, df_res_2, on='sid', how='inner')
    return merged_df


def or_op(df_res_1, df_res_2):
    df_res_1 = df_res_1.dropna(how='all')
    df_res_2 = df_res_2.dropna(how='all')
    merged_df3 = pd.concat([df_res_1, df_res_2], ignore_index=True)
    return merged_df3


def or_operation(result_1, result_2, r):
    # result=[]
    # for i in range(len(result_1)):
    #     for j in range(len(r)):
    #         if result_1[i]['sid']==r[j]['sid0']:
    #             sql = 'SELECT * FROM ' + st_name + f"1 WHERE sid = {r[j]['sid1']} ;"
    #             sec=execute_sql_query(sql)
    #             merged_result =result_1[i] | sec[0]
    #             print(merged_result)
    #             result.append(merged_result)
    # for k in range(len(result_2)):
    #     for j in range(len(r)):
    #         if result_2[k]['sid']==r[j]['sid1']:
    #             sql = 'SELECT * FROM ' + st_name + f"0 WHERE sid = {r[j]['sid0']} ;"
    #             fir=execute_sql_query(sql)
    #             merged_result =fir[0] | result_2[k]
    #             print(merged_result)
    #             result.append(merged_result)
    df1 = pd.DataFrame(result_1[2])
    merged_df1 = pd.merge(r, df1, left_on=result_1[1], right_on='sid', how="inner", suffixes=('', '_dump'))

    df2 = pd.DataFrame(result_2[2])
    merged_df2 = pd.merge(r, df2, left_on=result_2[1], right_on='sid', how="inner", suffixes=('', '_dump'))

    merged_df3 = pd.concat([merged_df1.filter(regex='^(?!.*_dump)'), merged_df2.filter(regex='^(?!.*_dump)')],
                           ignore_index=True)
    return merged_df3

    # return result


# 需要优化，需要知道传入数据对应的表对应在relation中sid_num
# 现在的sid_num是写死的
def single_condition(result_1):
    # result=[]
    df1 = pd.DataFrame(result_1[2])
    merged_df = pd.merge(df1, left_on=result_1[1], right_on='sid', how="inner", suffixes=('', '_dump'))
    # 将列表转换为字符串格式，用于SQL查询
    # id_tuple = tuple(merged_df[result_1[1]].values)
    # sql_query = f"SELECT * FROM {st_name} WHERE sid IN {id_tuple};"
    # sec = execute_sql_query(sql_query)
    # df2 = pd.DataFrame(sec)
    # merged_result = pd.merge(merged_df, df2, left_on='sid1', right_on='sid')
    # return merged_result
    return merged_df.filter(regex='^(?!.*_dump)')


# table_name = ''# data['table-config']['tables'][0]['real_table_name']    #可考虑是否优化配置文件
prefix = ''  # data['table-config']['tables'][2]['split_table_prefix']
rlationtabname = ''


def physical_single_deletion(sub_sqls, r, config):
    components = sub_sqls[0]
    # components = extract_delete_components(delete_sql[0])
    # print(components)
    select_sql = f"SELECT sid from {components[0]} where {components[3].value}"
    ids = execute_sql_query(select_sql)
    ids = pd.DataFrame(ids)
    df_sid = pd.merge(r, ids, left_on=components[1], right_on='sid', how="inner", suffixes=('', '_dump'))
    for table in config['split_tables']:
        if 'relaiton' not in table['split_table_name']:
            sid_list_str = ', '.join(map(str, df_sid[components[1]].tolist()))
            sql_2 = f"DELETE FROM {table} WHERE sid IN ({sid_list_str})"

    # print()
    # sql_1 = f"DELETE FROM {components['table_name']} {components['conditions']}"
    # sid_list_str = ', '.join(map(str, merged_df['sid0'].tolist()))
    # sql_2 = f"DELETE FROM {config['table-config']['tables'][1]['split_tables'][0]['split_table_name']} WHERE sid0 IN ({sid_list_str})"
    # sid_list_str = ', '.join(map(str, merged_df['sid1'].tolist()))
    # sql_3 = f"DELETE FROM {config['table-config']['tables'][1]['split_tables'][2]['split_table_name']} WHERE sid IN ({sid_list_str})"
    # excute delete sql
    # print(sql_1)
    # print(sql_2)
    # print(sql_3)


def destoryTree(res_tree):
    if isinstance(res_tree, pd.DataFrame):
        return res_tree
    i = 1
    while i < len(res_tree):
        if not isinstance(res_tree[i], tuple):
            res_tree[i] = destoryTree(res_tree[i])
        i += 1
    if isinstance(res_tree[0], Token) and res_tree[0].ttype is sqlparse.tokens.Keyword:
        if res_tree[0].value.upper() == 'AND':
            return and_op(res_tree[1], res_tree[2])
        elif res_tree[0].value.upper() == 'OR':
            return or_op(res_tree[1], res_tree[2])


def insert_2_tables(parse_single, stbmap, data):
    df = pd.DataFrame(parse_single['values'], columns=parse_single['columns'])
    conn = build_connection()
    try:
        # 创建游标对象
        with conn.cursor() as cur:
            # 执行 SQL 查询

            for col in parse_single['not_included_columns']:
                if data['split_tables'][0]['split_table_name'] == columnsMapsTab[col]:
                    uniqueset = set()
                    # 将生成的随机数添加到 DataFrame 中
                    # df[f'sid{col}'] = list(unique_numbers)
                    while len(uniqueset) < df.shape[0]:
                        unique_numbers = set(sample(range(100000000, 1000000000), df.shape[0] * 5))
                        r_list_str = ', '.join(map(str, list(unique_numbers)))
                        sql = f'Select {col} from {columnsMapsTab[col]} where {col} in ({r_list_str})'
                        print(f'\t{sql}')
                        cur.execute(sql)
                        res = cur.fetchall()
                        existing_sids = {item[col] for item in res}
                        unique_numbers.difference_update(existing_sids)
                        uniqueset = uniqueset.union(sample(list(unique_numbers), min(df.shape[0], len(unique_numbers))))

                    # uniqueset = np.array(list(map(str, uniqueset)))
                    # uniqueset = uniqueset.reshape(-1, 1)
                    df[col] = list(uniqueset)
                # else:
                #     df[col] = list(uniqueset)
                # results = np.hstack([df, uniqueset])
            # with open(json_file_path, 'r') as file:
            #     config = json.load(file)
            # extracted_data = parse_insert(sql)
            # col_list_1 = []
            # col_list_2 = []
            # col_list = []
            #
            # # 获取两个表中各自的列名列表
            # col_1 = config['table-config']['tables'][1]['split_tables'][1]['columns']
            # col_2 = config['table-config']['tables'][1]['split_tables'][2]['columns']
            #
            # for col in col_1:
            #     if col['name'] not in col_list_1:
            #         col_list_1.append(col['name'])
            # for col in col_2:
            #     if col['name'] not in col_list_2:
            #         col_list_2.append(col['name'])
            #
            # # 判断每个字段属于哪个表
            # for field in extracted_data['columns']:
            #     if field in col_list_1:
            #         col_list.append(1)
            #     elif field in col_list_2:
            #         col_list.append(2)

            # 遍历每组 values，构建对应的插入语句
            for table in stbmap:
                # for col in range(num_columns):
                #     unique_numbers = set()
                #     while len(unique_numbers) < num_rows:
                #         random_num = random.randint(100000000, 999999999)
                #         unique_numbers.add(random_num)
                #     # 将生成的随机数添加到 DataFrame 中
                #     # df[f'sid{col}'] = list(unique_numbers)
                #     unique_numbers = np.array(list(unique_numbers))
                #     unique_numbers = unique_numbers.reshape(-1, 1)
                #     results = np.hstack([results, unique_numbers])
                #     key_dict[f'sid{col}'] = len(key_dict.keys())

                # # 获取属于第一个表的字段和对应的值
                # first_indices = [index for index, v in enumerate(col_list) if v == 1]
                # first_fields = [extracted_data['columns'][i] for i in first_indices]
                # first_values = [value[i] for i in first_indices]
                #
                # # 获取属于第二个表的字段和对应的值
                # second_indices = [index for index, v in enumerate(col_list) if v == 2]
                # second_fields = [extracted_data['columns'][i] for i in second_indices]
                # second_values = [value[i] for i in second_indices]
                tempfields = stbmap[table][0]
                selected_columns = df[stbmap[table][0]]
                fields = tempfields[:]
                if table != data['split_tables'][0]['split_table_name']:
                    fields[0] = 'sid'
                # values = df[]
                #
                # indices = [index for index, v in enumerate(stbmap[0]) if v == 1]
                #
                # values = [value[i] for i in indices]
                # 构建插入语句
                # def construct_values(row):
                #     values = ', '.join([str(val) if val is not None else 'NULL' for val in row])
                #     return f"({values})"

                # 应用构造函数到每一行
                # values = selected_columns.apply(lambda row: construct_values(row), axis=1)
                result = selected_columns.apply(lambda row: f"({', '.join(str(row[col]) for col in tempfields)})",
                                                axis=1)
                sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES " + ",".join(result)
                
                cur.execute(sql)
                conn.commit()

                # first_sql = f"INSERT INTO server0 ({', '.join(first_fields)}) VALUES ({', '.join(first_values)})"
                # second_sql = f"INSERT INTO server1 ({', '.join(second_fields)}) VALUES ({', '.join(second_values)})"

                # # 输出结果
                # print("=======")
                # print("First Table SQL:", first_sql)
                # print("Second Table SQL:", second_sql)

    except Exception as e:
        print(f"exe_compose_query SQL 执行错误: {e}")
        return None
    finally:
        # 关闭数据库连接
        conn.close()


def compose_result(json_file_path, querysql):
    # 应该从一开始把sql解析完之后，把整个组件打包传递，避免重复解析
    parse_ans = parse_sql(querysql)

    data = read_config(json_file_path)

    # global table_name, prefix, rlationtabname
    # table_name = data['real_table_name']    #可考虑是否优化配置文件
    # prefix = data['split_table_prefix']
    # rlationtabname = data['split_tables'][0]['split_table_name']

    # r = get_relation(table_name + '_relation')   #这里相当于调用了一次sql全表查询，如果数据量较大的时候存在性能问题
    # print(r)
    cur_result = None
    for parse_single in parse_ans:
        if (parse_single['type'] in ('SELECT', 'UPDATE','DELETE') and parse_single['where_clause'] != None):
            con_tree, stbmap = generate_sub_sql_fromwhere(parse_single, data)
            res_tree = exeSqlTree(con_tree)
            SidTree = procRes2Sid(res_tree)
            # backup_tree = SidTree
            df_sids = destoryTree(SidTree)
            # df_sidx = select_R(df_sids)
            if parse_single['type'] == 'SELECT':
                cur_result = exe_compose_query(stbmap, df_sids, data, False)
            elif parse_single['type'] == 'DELETE':
                exe_compose_delete(stbmap, df_sids, data, False)
        elif parse_single['type'] == 'SELECT':
            stbmap = init_stbmap(parse_single, data)
            cur_result = exe_fulltable_query(parse_single, stbmap, data, False)
        elif parse_single['type'] == 'DELETE':
            # 　cur_result = exe_compose_query(stbmap, df_sids, data, False)#全表
            pass  # 全表删除这种大杀器先不开发了
        elif parse_single['type'] == 'INSERT':
            stbmap = init_stbmap(parse_single, data)
            # df_values = prepare_values(parse_single, stbmap, data)
            insert_2_tables(parse_single, stbmap, data)

        #     if len(query_results)==1:
        #         df_sidx = single_condition(query_results[0])
        #     else:
        #         operator = parse_single['where_clause'][1]
        #
        #         cur_result= query_results[0]
        #         if operator.value in "AND":
        #             tar_result= query_results[1]
        #             df_sidx = and_operation(cur_result, tar_result)
        #         if operator.value in "OR":
        #             tar_result = query_results[1]
        #             df_sidx = or_operation(cur_result, tar_result)  # 使用了分条查询，性能瓶颈
        #     cur_result = exe_compose_query(stbmap, df_sidx, data)
        # elif parse_single['type'] == 'DELETE':
        #     if len(sub_sql_tree) == 1:
        #         # single_delete
        #         # print(sub_delete)
        #         print("单一条件")
        #         physical_single_deletion(sub_sql_tree, data)
        #     else:
        #         operator = parse_single['where_clause'][1]  # 应对大小写不敏感(已处理)
        #         cur_result = query_results[0]
        #     # cur_result = exe_compose_delete(stbmap, df_sidx, data)

        #     if operator.value in "AND":
        #         df_sidx = physical_and_deletion(sub_sqls, r, data)
        #     if operator.value in "OR":
        #         df_sidx = physical_or_deletion(sub_sqls, r, data)
    return cur_result


# 测试生成子查询的方法
test_query9 = '''INSERT INTO sada_gdpi_click_dtl (
    sid,
    f_srcip,
    f_ts,
    f_src_port,
    f_json,
    f_update_time,
    f_url,
    f_ua,
    f_dstip,
    f_cookie,
    f_ref,
    f_ad
) VALUES (
    123018,
    'value_for_f_srcip',
    'value_for_f_ts',
    'value_for_f_src_port',
    'value_for_f_json',
    'value_for_f_update_time',
    'value_for_f_url',
    'value_for_f_ua',
    'value_for_f_dstip',
    'value_for_f_cookie',
    'value_for_f_ref',
    'value_for_f_ad'
),
(
    123019,
    'value_for_f_srcip',
    'value_for_f_ts',
    'value_for_f_src_port',
    'value_for_f_json',
    'value_for_f_update_time',
    'value_for_f_url',
    'value_for_f_ua',
    'value_for_f_dstip',
    'value_for_f_cookie',
    'value_for_f_ref',
    'value_for_f_ad'
);'''
test_query0 = '''
SELECT f_srcip, f_dstip, f_src_port, f_ad FROM sada_gdpi_click_dtl WHERE f_dstip = '103.37.155.60' and  f_ad = 'f18b267140e1a0491a635fb42ce7ae41329d0f2287c22a5074b6' 
'''
test_query1 = '''
SELECT f_srcip, f_dstip, f_src_port, f_ad FROM sada_gdpi_click_dtl WHERE f_srcip = '224.93.65.63' OR f_ad = 'f18b2b7946e4a22f186a6fd59d67d0c6dfca4fa363d303fd722a' 
'''
test_query2 = '''
SELECT f_srcip, f_dstip, f_src_port, f_ad FROM sada_gdpi_click_dtl WHERE f_dstip = '103.37.155.60'
'''

test_query3 = '''
SELECT * FROM sada_gdpi_click_dtl WHERE f_dstip='103.37.155.60' and f_src_port='CF56' OR f_ad = 'f18b267140e1a0491a635fb42ce7ae41329d0f2287c22a5074b6'  and f_src_port='CF56' 
'''
test_query4 = '''SELECT * FROM sada_gdpi_click_dtl WHERE f_dstip='103.37.155.60';
'''

test_query5 = '''
DELETE FROM sada_gdpi_click_dtl WHERE f_dstip = '103.37.155.60' OR f_ad = 'f18b267140e1a0491a635fb42ce7ae41329d0f2287c22a5074b6'
'''

test_query6 = '''
SELECT * FROM sada_gdpi_click_dtl WHERE f_dstip = '123'
'''

test_query7 = '''
INSERT INTO sada_gdpi_click_dtl (
    sid,
    f_srcip,
    f_ts,
    f_src_port,
    f_json,
    f_update_time,
    f_url,
    f_ua,
    f_dstip,
    f_cookie,
    f_ref,
    f_ad
) VALUES (
    123018,
    'value_for_f_srcip',
    'value_for_f_ts',
    'value_for_f_src_port',
    'value_for_f_json',
    'value_for_f_update_time',
    'value_for_f_url',
    'value_for_f_ua',
    'value_for_f_dstip',
    'value_for_f_cookie',
    'value_for_f_ref',
    'value_for_f_ad'
),
(
    123019,
    'value_for_f_srcip',
    'value_for_f_ts',
    'value_for_f_src_port',
    'value_for_f_json',
    'value_for_f_update_time',
    'value_for_f_url',
    'value_for_f_ua',
    'value_for_f_dstip',
    'value_for_f_cookie',
    'value_for_f_ref',
    'value_for_f_ad'
);
DELETE FROM sada_gdpi_click_dtl WHERE f_dstip = 'value_for_f_dstip';
'''
test_query8 = '''
DELETE FROM sada_gdpi_click_dtl
'''

test_query10 = '''
DELETE FROM sada_gdpi_click_dtl
'''
test_query11 = '''
SELECT password, user_id FROM meeting100w WHERE password = 'XtZDJBtK67';
'''
test_query12 = '''
select * from t_user_info;
'''

test_query13 = '''
select * from customer_msg where cust_id = '202139563193';
'''

test_query14 = '''
select * from t_user_info where id='20';
'''
test_query15 = '''delete from t_user_info where user_id = '2443f7b6d72c47e9ac8f84b0de237f87';'''

test_query16 = '''SELECT device_identifier FROM meeting10w WHERE meeting_time >= '2017-01-07 22:17:33' OR email = 'lcy7IRkwEwA@163.com';'''
# start_time = time.time()
# compose_result('../jd/3/table_config.json', test_query15)

test_query20 = '''
SELECT * FROM meeting500w WHERE device_identifier = 'BE6C68AB011B267AF0F444FFFE3D4089BCB26549' 
'''



test_query21 = '''
SELECT user_id, meeting_host, name, email FROM meeting500w WHERE meeting_host = '焦正材'  OR device_identifier = 'BE6C68AB011B267AF0F444FFFE3D4089BCB26549' AND meeting_time >= '2023-08-15 21:12:40' OR email = 'yIUquLmXGVlKOLTb@qq.com'
'''



test_query22 = '''
SELECT user_id, meeting_host, name, email FROM meeting500w WHERE meeting_host = '焦正材'  OR device_identifier = 'BE6C68AB011B267AF0F444FFFE3D4089BCB26549' AND meeting_time >= '2023-04-25 04:58:29' OR email = 'yIUquLmXGVlKOLTb@qq.com'
'''

test_query23 = '''
SELECT user_id, meeting_host, name, email FROM meeting500w WHERE meeting_host = '焦正材'  OR device_identifier = 'BE6C68AB011B267AF0F444FFFE3D4089BCB26549' AND meeting_time >= '2020-07-18 09:49:56' OR email = 'yIUquLmXGVlKOLTb@qq.com'
'''

print(compose_result('table_config.json', test_query23))
#
# # data = read_config('../2/table_config.json')
# # extract_sql_components(test_query4, data)
# #
# # # 记录结束时间
# end_time = time.time()
# #
# # # 计算运行时间
# elapsed_time = end_time - start_time
# print(f"The function took {elapsed_time:.4f} seconds to run.")
#
