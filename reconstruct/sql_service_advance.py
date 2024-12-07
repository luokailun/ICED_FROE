import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison, Where, Token, Function, Parenthesis, Values
from sqlparse.tokens import Keyword, DML, Wildcard, Operator
import json
import time
import pandas as pd
from collections import defaultdict
from random import *

import numpy as np
import pymysql

import connection_test
from conditions import Conjunction, Disjunction

##YES
def build_connection():

    # 配置数据库连接参数
    # connection = pymysql.connect(
    #     host='localhost',  # 数据库主机地址
    #     user='root',  # 数据库用户名
    #     password='fenghou',  # 数据库密码
    #     database='sada',  # 数据库名称
    #     port=3306,  # MySQL 默认端口
    #     cursorclass=pymysql.cursors.DictCursor  # 返回结果为字典形式
    # )

    # connection = pymysql.connect(
    #     host='127.0.0.1',
    #     user='root',
    #     port=3306,
    #     password='fenghou',
    #     database='sada',
    #     cursorclass = pymysql.cursors.DictCursor  # 返回结果为字典形式
    # )
    # connection = pymysql.connect(
    #     host='arm.dgut.bar',
    #     user='root',
    #     port=3306,
    #     password='fD4!pL8@nV$bX3^mZk',
    #     database='dgut',
    #     cursorclass = pymysql.cursors.DictCursor  # 返回结果为字典形式
    # )
    #print("##Only for Testing....connection_test")
    #return connection_test.Connection_test()

    connection = pymysql.connect(
        host='172.31.150.60',
        user='root',
        port=7487,
        password='l84kCG5KP4uNNtRX',
        database='meetingData',
        cursorclass=pymysql.cursors.DictCursor
    )
    # connection = pymysql.connect(
    #     host='localhost',
    #     user='dgut',
    #     port=3306,
    #     password='dgut891014',
    #     database='meetingData',
    #     cursorclass=pymysql.cursors.DictCursor
    # )

    return connection



#############################################################################################################################

# def parse_column_details_on(token):
#     """提取 ON 子句中的列名和表别名（如果有）"""
#     if isinstance(token, Identifier):
#         if '.' in str(token):
#             table_alias, column_name = str(token).split('.', 1)
#             return table_alias.strip(), column_name.strip()
#         else:
#             return None, str(token).strip()
#     return None, None


# def extract_on_clause_conditions(parsed):
#     """提取 ON 子句中的条件和表别名、列名"""
#     conditions = []
#     columns = []
#     on_clause_detected = False

#     for token in parsed.tokens:
#         if token.ttype is Keyword and token.value.upper() == 'ON':
#             on_clause_detected = True
#             continue

#         if on_clause_detected:
#             if isinstance(token, Comparison):
#                 conditions.append(str(token).strip())
#                 for sub_token in token.tokens:
#                     if isinstance(sub_token, Identifier):
#                         table_alias, column_name = parse_column_details_on(sub_token)
#                         if column_name:
#                             columns.append((table_alias, column_name))

#     return conditions, columns


# def extract_on_conditions_from_sql(sql):
#     """从SQL查询中提取 ON 子句条件，并获取表别名和列名"""
#     parsed = sqlparse.parse(sql)
#     on_clause_conditions = []
#     on_clause_columns = []

#     for stmt in parsed:
#         if stmt.get_type() == 'SELECT':
#             conditions, columns = extract_on_clause_conditions(stmt)
#             if conditions:
#                 on_clause_conditions.extend(conditions)
#             if columns:
#                 on_clause_columns.extend(columns)

#     return on_clause_conditions, on_clause_columns


#############################################################################################################################





#############################################################################################################################
###YES
def parse_sql(sql):
    parsed_statements = sqlparse.parse(sql)
    result_list = []

    for statement in parsed_statements:
        result = parse_statement(statement)
        result_list.append(result)

    return result_list

###YES
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

###YES
def parse_select(statement):
    """递归解析SELECT语句"""
    fields = []
    table_names = []
    where_clause = None
    from_seen = False
    seen_From = False

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

    return {
        'type': 'SELECT',
        'fields': fields,
        'table_name': table_names,
        'where_clause': where_clause#,
        #'ori_where_clause': ori_where_clause
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


from collections import defaultdict


###YES
def parse_where(where_token):
    """递归解析WHERE子句"""
    stacks = []
    #stacks.append('#')
    conditions = []
    i = 0
    #print(where_token.tokens)
    
    for token in where_token.tokens:
        if token.ttype is sqlparse.tokens.Whitespace or not token.value.strip() or token.value.upper() == 'WHERE':
            i += 1
            continue  # 跳过空白字符和where
        else:
            if token.value.upper() in ('AND'):  # sqlparse并不能解析出左右括号
                continue
            elif token.value.upper() in ('OR'):
                conditions.append(stacks)
                stacks = []
            else:
                stacks.append(token)
                #print(stacks)
        i += 1

    if stacks!=[]:
        conditions.append(stacks)

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


#############################################################################################################################



def init_stbmap(parse_sin,data):

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
        for col,tbname in original_table_structure:
            if col in parse_sin['columns']:
                stbmap[tbname][0].append(col)
            elif 'relation' in tbname:
                parse_sin['not_included_columns'].append(col)
                stbmap[tbname][0].append(col)

        # column_list.append(f"{original_table_structure[field_name]}.{field_name}")
        # field_names.update(column_list)
        # for field in extract_select_fields(stmt):
        #     field_names.add(field)
    # print(stbmap)
   
    return stbmap



class ColumnMapping:
    def __init__(self, column_name, table_name):
        self.column_name = column_name
        self.table_name = table_name


original_table_structure = []
columnsMapsTab = defaultdict(str)
relatesidx = defaultdict(str)
idName = ""


###YES
def read_config(json_file):
    global original_table_structure, columnsMapsTab, idName
    original_table_structure = []
    global flag_hash

    try:
        with open(json_file, 'r') as input_stream:
            data = json.load(input_stream)

            server_num = len(data["split_tables"])-1
            if flag_hash is True:
                surfix_for_query = f"_n{server_num}_hash"
            else:
                surfix_for_query = f"_n{server_num}"

            idName = f"{data['split_tables'][0]['split_table_name']}{surfix_for_query}"
            #data['split_tables'][0]['split_table_name'] = idName

            ori_table_name = data['real_table_name']

            for split_table in data["split_tables"]:
                split_table_name = f"{split_table['split_table_name']}{surfix_for_query}"
                split_table['split_table_name'] = split_table_name

                columns = split_table["columns"]
                relatesidx[split_table_name] = split_table['relate_subid']
                for column in columns:
                    column_name = column.get("name")
                    original_table_structure.append((column_name,split_table_name))  # 使用字典映射
                    columnsMapsTab[column_name] = split_table_name
            return data
    except IOError as e:
        print(f"文件读取错误: {e}")
    except Exception as e:
        print(e)

    except IOError as e:
        print(f"文件读取错误: {e}")
    except Exception as e:
        print(e)



#############################################################################################################################
###  解释：
###  stbmap： 记录每个分表需要什么属性， 例如{'meeting100w_server0': (['user_id', 'meeting_host'], [])}
###  columnsMapsTab：记录属性隶属于哪个分表， 例如：{'sid1': 'meeting100w_relation'}
###  original_table_structure: 记录所有属性与分表的关系：如： ('sid0', 'meeting100w_relation') 的list
###
###
#############################################################################################################################




#############################################################################################################################

def __get_conjunct_server_pred_structure(DNF_formula, attr2server_dict):

    conjunct_list = list()
    for conjunct in DNF_formula:
        preds = [ cmp_object.value for cmp_object in conjunct]
        con = Conjunction(preds, attr2server_dict)
        conjunct_list.append(con)

    return conjunct_list


def generate_DNF_structure_fromwhere(parse_sin, data):


    global relatesidx

    stbmap = init_stbmap(parse_sin, data)
    
    ####server2attrs_dict没用到？
    # server2attrs_dict = {server: list() for server in stbmap.keys() if server.find('_relation')==-1}
    # for attr, server in original_table_structure:
    #     if server.find('_relation')==-1:
    #         server2attrs_dict[server].append(attr)
    attr2server_dict = {attr: server for attr, server in original_table_structure }

    conjunct_list = __get_conjunct_server_pred_structure(parse_sin['where_clause'], attr2server_dict)
    
    DNF = Disjunction(conjunct_list, stbmap, relatesidx)
   
    return DNF







#############################################################################################################################



def compose_result(json_file_path, querysql, Executor):
    # 应该从一开始把sql解析完之后，把整个组件打包传递，避免重复解析
    global relatesidx

    parse_ans = parse_sql(querysql)

    data = read_config(json_file_path)

    relation_table_name = data['split_tables'][0]['split_table_name']
    unifed_sid_name = data['split_tables'][0]['columns'][0]['name']


    connection = build_connection()

    for parse_single in parse_ans:

        if (parse_single['type'] in ('SELECT','UPDATE') or
                parse_single['type'] == ('DELETE') and parse_single['where_clause'] != None):

            DNF = generate_DNF_structure_fromwhere(parse_single, data)



            # sid_DNF = generate_sid_DNF(DNF, relation_table_name, unifed_sid_name, relatesidx)  # 每次执行sql的时候都会重建连接
            # df_sids = combine_sids(sid_DNF)
            # if parse_single['type'] == 'SELECT':
            #     cur_result = exe_compose_query(DNF, df_sids, data, False)
            
            # executor = QueryLeastExecutor(data, relation_table_name, unifed_sid_name, DNF, connection)
            # cur_result = executor.execute()
            executor = Executor(data, relation_table_name, unifed_sid_name, DNF, connection)
            cur_result = executor.execute()


        elif parse_single['type'] == 'DELETE':
            cur_result = exe_compose_query(DNF, df_sids, data, False)
        elif parse_single['type'] == 'INSERT':
            stbmap = init_stbmap(parse_single, data)
            # df_values = prepare_values(parse_single, stbmap, data)
            insert_2_tables(parse_single, stbmap, data)

    connection.close()    


    return pd.DataFrame(cur_result)



#############################################################################################################################
#############################################################################################################################
#############################################################################################################################

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
test_query4 = '''
SELECT * FROM sada_gdpi_click_dtl WHERE f_dstip='103.37.155.60'
'''

test_query5 = '''
DELETE FROM sada_gdpi_click_dtl WHERE f_dstip = '103.37.155.60' OR f_ad = 'f18b267140e1a0491a635fb42ce7ae41329d0f2287c22a5074b6'
'''

test_query6 = '''
SELECT * FROM sada_gdpi_click_dtl WHERE f_dstip = '123'
'''

test_query7 = '''
DELETE FROM sada_gdpi_click_dtl WHERE f_dstip = '103.37.155.60'
'''
test_query8 = '''
DELETE FROM sada_gdpi_click_dtl
'''

test_query10 = '''
DELETE FROM sada_gdpi_click_dtl
'''
test_query11 = '''
SELECT user_id, meeting_host, name, email FROM meeting100w WHERE meeting_host = '焦正材';
'''
test='''SELECT * from meeting100w where phone_number = 18530558350'''


test_query12 = '''
SELECT user_id, meeting_host, name, email FROM meeting100w WHERE meeting_host = '焦正材' And phone_number = 18530558350 
'''

test_query13 = '''
SELECT * FROM meeting100w WHERE meeting_host = '焦正材' And phone_number = 18530558350 
'''


test_query14 = '''
SELECT user_id, meeting_host, name, email FROM meeting100w WHERE meeting_host = '焦正材' OR phone_number = 18530558350
'''


test_query15 = '''
SELECT user_id, meeting_host, name, email FROM meeting100w WHERE meeting_host = '焦正材'  OR name = '班昕诗' AND phone_number = 18530558350 OR email = "aaa"
'''

test_query16 = '''
SELECT user_id, meeting_host, name, email FROM meeting100w WHERE meeting_host = '焦正材'  OR phone_number = 18530558350 OR email = "TdpfuKz3lQeE@163.com"
'''

test_query17 = '''
SELECT user_id, meeting_host, email FROM meeting100w WHERE meeting_host = '焦正材'  OR phone_number = 18530558350 OR email = "TdpfuKz3lQeE@163.com"
'''


test_query18 = '''
SELECT user_id, meeting_host, name, email FROM meeting500w WHERE meeting_host = '焦正材'  OR name = '班昕诗' AND phone_number = 18530558350 OR email = "aaa"
'''

test_query19 = '''

SELECT meeting_host, name, user_id, phone_number FROM meeting100w WHERE sms_verification_code >= 1000

'''

test_query20 = '''
SELECT user_id, meeting_host, name, email FROM meeting500w WHERE meeting_host = '焦正材'  OR device_identifier = 'BE6C68AB011B267AF0F444FFFE3D4089BCB26549' AND meeting_time >= '2023-08-15 21:12:40' OR email = 'yIUquLmXGVlKOLTb@qq.com'
'''


test_query21 = '''
SELECT * FROM meeting500w WHERE device_identifier = 'BE6C68AB011B267AF0F444FFFE3D4089BCB26549' 
'''



test_query22 = '''
SELECT user_id, meeting_host, name, email FROM meeting500w WHERE meeting_host = '焦正材'  OR device_identifier = 'BE6C68AB011B267AF0F444FFFE3D4089BCB26549' AND meeting_time >= '2023-04-25 04:58:29' OR email = 'yIUquLmXGVlKOLTb@qq.com'
'''

test_query23 = '''
SELECT user_id, meeting_host, name, email FROM meeting500w WHERE 
meeting_host = '焦正材'  OR 
device_identifier = 'BE6C68AB011B267AF0F444FFFE3D4089BCB26549' 
AND meeting_time >= '2022-02-18 09:49:56' OR 
email = 'yIUquLmXGVlKOLTb@qq.com'
'''



# compose_result('table_config.json', test)

start_time = time.time()

from executor import BasicExecutor, QueryLeastExecutor
# hash_value = hash(12345)
# print(hash_value)
# exit(0)


flag_hash= False

result = compose_result('./table_config.json', test_query23, QueryLeastExecutor)
print("\n\n##---------------\n")
print(result)
print("\n##---------------\n")



# 计算运行时间
elapsed_time = time.time() - start_time
print(f"The function took {elapsed_time:.4f} seconds to run.")




