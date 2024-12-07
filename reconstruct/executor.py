

import pandas as pd


class Executor(object):
	"""docstring for Executor"""
	def __init__(self, data, relation_table_name, unifed_sid_name, DNF_structure,connection):
		super(Executor, self).__init__()
		self.data = data
		self.relation_table_name = relation_table_name
		self.unifed_sid_name = unifed_sid_name
		self.DNF_structure = DNF_structure
		self.connection = connection


	def AvoidNullResult(self, cur, sql):
	    #print(f"executing....\n.  {sql}")
	    cur.execute(sql)

	    # 获取查询结果
	    result = cur.fetchall()

	    print(len(result))
	    # 获取列名
	    column_names = [desc[0] for desc in cur.description]

	    # 检查结果集是否为空
	    if not result:
	        # 创建一个空的字典列表，其中包含所有列名作为键
	        result = [{column: None for column in column_names}]
	    return result

	def AvoidNullResult2(self, cur, sql):
	    print(f"@executing....\n.  {sql}")
	    cur.execute(sql)

	    # 获取查询结果
	    result = cur.fetchall()

	    # 获取列名
	    column_names = [desc[0] for desc in cur.description]

	    # 检查结果集是否为空
	    if not result:
	        return []
	    return result

	def execute_sql_list(self, sql_list, cur):

	    results = list()
	    for sql in sql_list:
	        if sql is not None:
	            result = self.AvoidNullResult(cur, sql)
	            results.append(pd.DataFrame(result))
	        else:
	            results.append(None)
	    return results


	def execute_DNF_sql(self, DNF_sql, cur):
	    
	    result_list = list()
	    for sql_list in DNF_sql:
	        results = self.execute_sql_list(sql_list, cur)
	        result_list.append(results)
	    return result_list

	def and_operation(self, df_res_1, df_res_2):
	    # sids1 = set(df_res_1['sid'])
	    # sids2 = set(df_res_2['sid'])
	    #
	    # common_sids = sids1 & sids2
	    # # 将结果转换回 DataFrame
	    # result_df = pd.DataFrame(list(common_sids), columns=['sid'])
	    merged_df = pd.merge(df_res_1, df_res_2, on='sid', how='inner')
	    return merged_df


	def or_operation(self, df_res_1, df_res_2):
	    merged_df3 = pd.concat([df_res_1, df_res_2], ignore_index=True)
	    return merged_df3



class BasicExecutor(Executor):


	def execute(self):

		data = self.data
		relation_table_name = self.relation_table_name
		unifed_sid_name = self.unifed_sid_name

		sid_DNF = self.__generate_sid_DNF(relation_table_name, unifed_sid_name)  
		df_sids = self.__combine_sids(sid_DNF)
		cur_result = self.__exe_compose_query(df_sids, data, False)

		return cur_result


	def __generate_sid_DNF(self, relation_table_name, unifed_sid_name):

	     # 创建游标对象   
	    cur = self.connection.cursor()

	    subsid_sqls_DNF = self.DNF_structure.generate_subsid_sql()
	    subsid_DNF = self.execute_DNF_sql(subsid_sqls_DNF, cur)
	    sid_sqls_DNF = self.DNF_structure.generate_unified_sid_sql(subsid_DNF, relation_table_name, unifed_sid_name)
	    sid_DNF = self.execute_DNF_sql(sid_sqls_DNF, cur)
	    
	    return sid_DNF


	def __combine_sids(self, sid_DNF):
    
	    and_df_list = list()
	    for sid_conjunct in sid_DNF:
	        and_df = sid_conjunct[0]  # 初始化为第一个 DataFrame
	        for df in sid_conjunct[1:]:
	            if df is not None:
	                and_df = self.and_operation(and_df, df)
	        and_df_list.append(and_df)


	    or_df = and_df_list[0]
	    for df in and_df_list[1:]:
	        if df is not None:
	            or_df = self.or_operation(or_df, df)
	    return or_df


	def __exe_compose_query(self, df_sids, data, bool_df=True):

	    cur = self.connection.cursor()

	    relation_table_name = data['split_tables'][0]['split_table_name']
	    unifed_sid_name = data['split_tables'][0]['columns'][0]['name']

	    #print(df_sids)

	    if df_sids is not None:
	        r_list_str = ', '.join(map(str, df_sids[unifed_sid_name].tolist()))
	    else:
	        r_list_str = 'None'

	    if r_list_str != 'None':
	        sql = f"SELECT * FROM {relation_table_name} WHERE {unifed_sid_name} IN ({r_list_str})"
	    else:
	        sql = f"SELECT * FROM {relation_table_name} WHERE {unifed_sid_name} = -1"

	    full_sids = self.AvoidNullResult(cur, sql)
	    sql_list = self.DNF_structure.generate_server_required_sqls(full_sids)
	    results = self.execute_sql_list(sql_list, cur)
	    
	    return self.DNF_structure.connect_result(full_sids, data, results, bool_df)


# class BasicExecutor(Executor):


from number import ExtendedInfinity
from score import ScoreQueue


class QueryLeastExecutor(Executor):


	def __init_query_server_results(self, server_results, attr_servers):
		for server in attr_servers:
			server_results[server] = dict()

	def __init_conjuncts_sidx(self, conjuncts_sidx, conjuncts):

		for conjunct in conjuncts:
			conjuncts_sidx[conjunct] = dict()

	def __init_conjuncts_value(self, conjuncts_value, conjuncts):
		for conjunct in conjuncts:
			conjuncts_value[conjunct] = ExtendedInfinity()


	def __get_server_query_attrs_str(self, require_attrs):

		if len(require_attrs)>0:
			query_attrs = "sid, %s"%(", ".join(require_attrs))
		else:
			query_attrs = "sid"
		return query_attrs


	def get_combined_dict(self, conjuncts_sidx, conjuncts_value, unifed_sid_name):

		new_dict = {}
		for conjunct, conjunct_sidx in conjuncts_sidx.items():
			if conjuncts_value[conjunct]>0:
				new_dict = {**new_dict, **conjunct_sidx[unifed_sid_name]}

		return new_dict




	def init_queue_score(self, query_servers, conjuncts_value, squeue):
		for server in query_servers:
			score = self.DNF_structure.get_server_score(server, conjuncts_value)
			squeue.push(server, score)



	def update_queue_score(self, conjuncts_value, squeue):

		squeue.update_scores(self.DNF_structure.get_server_score, conjuncts_value)


	def get_restricted_sidx(self, sidx_dict, unifed_sid_name, server_related_sidx):

		items = sidx_dict[unifed_sid_name].values()
		restricted_ids = [ str(item[server_related_sidx]) for item in items]
		return restricted_ids


	def create_conjunct_sidx_from_resutls(self, conjunct_sidx, \
				relation_table_name, related_sidx, results):

		item_ids = [str(item['sid']) for item in results]
		str_ids = ", ".join(item_ids)

		sql = f"SELECT * from {relation_table_name} where {related_sidx} in ({str_ids})"

		cur = self.connection.cursor()
		sidx_results = self.AvoidNullResult2(cur, sql)

		for key in sidx_results[0].keys():
			conjunct_sidx[key] = { item[key]: item for item in sidx_results}


	def __restricted_query_and_modify_server_result(self, str_sidx, server, \
				query_attrs, query_server_results ):

		where_condition = f"sid in ({str_sidx})"
		sql = f"SELECT {query_attrs} FROM {server} WHERE {where_condition}"

		cur = self.connection.cursor()
		results = self.AvoidNullResult2(cur, sql)

		result_strcture = {item['sid']: item for item in results}
		query_server_results[server].update(result_strcture)


	def makeup_server_result(self, query_server_results, conjunct_sidx, unifed_sid_name, server, makeup_sidx, query_attrs):
						
		#sidxs = [ str(record[makeup_sidx]) for record in conjunct_sidx[unifed_sid_name]]
		sidxs = self.get_restricted_sidx(conjunct_sidx, unifed_sid_name, makeup_sidx)

		str_sidx = ", ".join(sidxs)
		#sidx_name = makeup_sidx

		self.__restricted_query_and_modify_server_result(str_sidx, server, query_attrs,  query_server_results )

	def delete_server_result(self, query_server_results, executed_server, delete_sidx):

		for sidx in delete_sidx:
			query_server_results[executed_server].pop(sidx, None)

	# @update Si operation: 
	# use sid0 to delete this set sid0_set: 
	# 	-- update mdict["sid0"] by delete id not in sid0_set
	# 	    " sid0": { i: mdict["sid0"][i]  for i in sid0_set}
	# 	-- find all the sid in mdict["sid0"]: 
	#   		 sid_set = [item['sid']	for item in mdict["sid0"].valus()]
	#   -- update mdict["sid0"] by delete id not in sid_set
	
	def update_conjunct_sidx(self, conjunct_sidx, unifed_sid_name, related_sidx, results, flag_delete):
		#get update sidx
		item_ids = [item['sid'] for item in results]
		#update sidx
		conjunct_sidx[related_sidx] = { i: conjunct_sidx[related_sidx][i] for i in item_ids}
		#get update sid
		update_sids = [item[unifed_sid_name] for item in conjunct_sidx[related_sidx].values()]

		# get delete results:
		if flag_delete is True:
			update_sids = set(update_sids)
			delele_sidx = [ item for i, item in conjunct_sidx[unifed_sid_name].items() 
			if i not in update_sids ]
		else:
			delele_sidx = []

		#update sid
		conjunct_sidx[unifed_sid_name] = { i: conjunct_sidx[unifed_sid_name][i] for i in update_sids}

		return delele_sidx


	# def get_delete_results(conjunct_sidx, related_sidx, results):

	# 	item_ids = [item[related_sidx] for item in results]
	# 	conjunct_sidx[related_sidx] = { i: conjunct_sidx[related_sidx][i] for i in item_ids}


	def execute_server_query(self, query_server_results,  server, \
		related_sidx, require_attrs, conjunct_preds, conjuncts_sidx, conjuncts_value):

		flag_make_up = False
		makeup_list = list()
		flag_delete = False
		delete_dict = {}
		sum_id_list = list()

		query_attrs  = self.__get_server_query_attrs_str(require_attrs)

		#print("for server %s"%server)

		for conjunct, preds in conjunct_preds.items():
			## this conjunct has been false:

			if conjuncts_value[conjunct] == 0:
				continue
			## if the predicate for server affects the conjunct:
			if len(preds) > 0:
				where_condition = ' AND '.join([ pred for pred in preds])

				if isinstance(conjuncts_value[conjunct], ExtendedInfinity):
					flag_make_up =  True
				else:
					sidx_dict = conjuncts_sidx[conjunct]
					sidx = self.get_restricted_sidx(sidx_dict, self.unifed_sid_name, related_sidx)
					str_sidx = ", ".join(sidx)
					where_condition = f"sid in ({str_sidx}) AND {where_condition}"
					flag_delete = True

				sql = f"SELECT {query_attrs} FROM {server} WHERE {where_condition}"

				cur = self.connection.cursor()
				results = self.AvoidNullResult2(cur, sql)

				#update conjunct_value
				conjuncts_value[conjunct] = len(results)
				#if there is no conjunct_sidx, create one
				if flag_make_up is True and len(results)>0:
					self.create_conjunct_sidx_from_resutls(conjuncts_sidx[conjunct], \
							self.relation_table_name, related_sidx, results)
					makeup_list.append(conjunct)


				#update conjunct_sidx
				delete_items = self.update_conjunct_sidx(conjuncts_sidx[conjunct], \
							self.unifed_sid_name, related_sidx, results, flag_delete)

				if flag_delete is True and len(delete_items)>0:
					delete_dict[conjunct] = delete_items

				if server in query_server_results:
				#update server result
					result_strcture = {item['sid']: item for item in results}
					query_server_results[server].update(result_strcture)

			else:

				if not isinstance(conjuncts_value[conjunct], ExtendedInfinity) \
					and server in query_server_results:

					sidx_dict = conjuncts_sidx[conjunct]
					sidx = self.get_restricted_sidx(sidx_dict, self.unifed_sid_name, related_sidx)
					sum_id_list.extend(sidx)

		if len(sum_id_list)!=0:

			sum_id_str = ", ".join(sum_id_list)
			self.__restricted_query_and_modify_server_result(sum_id_str, \
						server, query_attrs, query_server_results)
			

		return makeup_list, delete_dict


	def connect_result(self, sidx_items, query_server_results, server_related_sidx, bool_df=False):
		merged_df = pd.DataFrame(sidx_items)  # 初始化 merged_df 为 sidx_items

		# print(query_server_results)
		# exit(0)
		for server, result in query_server_results.items():
			df_result = pd.DataFrame(result.values())
			related_sidx = server_related_sidx[server]
			# print(merged_df)
			# print(df_result)
			# print(server)
			# print(related_sidx)
			merged_df = pd.merge(merged_df, df_result, left_on=related_sidx, right_on='sid',
                                       how="inner", suffixes=('', '_dump'))

		#过滤
		regstr = '^(?!.*_dump)(?!sid[0-9]+)'
		merged_df = merged_df.filter(regex=regstr)
		return merged_df if bool_df else merged_df.to_dict(orient='records')



	def execute(self):

		query_server_results = dict()
		conjuncts_sidx = dict()
		conjuncts_value = dict()

		query_servers = self.DNF_structure.get_query_servers()
		conjuncts = self.DNF_structure.get_conjuncts()
		server_conjunct_preds = self.DNF_structure.get_server_conjunct_preds()
		server_require_attrs = self.DNF_structure.get_server_required_attrs()
		server_related_sidx = self.DNF_structure.get_server_relatedsidx()
		attr_servers = server_require_attrs.keys()
		## there might be some server need attrs but not in query_servers
		self.__init_query_server_results(query_server_results, attr_servers)
		self.__init_conjuncts_sidx(conjuncts_sidx, conjuncts)
		self.__init_conjuncts_value(conjuncts_value, conjuncts)


		squeue = ScoreQueue()
		executed_servers = list()
		self.init_queue_score(query_servers, conjuncts_value, squeue)

		while squeue.is_empty() is not True:
			
			server, score = squeue.pop()

			print("@executing server ....")
			print(server, score)
			# exit(0)
			conjunct_preds = server_conjunct_preds[server]
			require_attrs = server_require_attrs[server]
			related_sidx = server_related_sidx[server]
			makeup_list, delete_dict = self.execute_server_query(query_server_results,  server, \
					related_sidx, require_attrs, conjunct_preds, conjuncts_sidx, conjuncts_value)

			if len(makeup_list)>0:

				print("@making up ....")
				for executed_server in executed_servers:
					if executed_server in query_server_results:
						print(f"@making up result for .... {executed_server}")
						makeup_sidx = server_related_sidx[executed_server]
						query_attrs = self.__get_server_query_attrs_str(server_require_attrs[executed_server])
						# make the result of executed_server for each affected conjuncts
						for conjunct in makeup_list:
							# how many sidx should be make up of 
							conjunct_sidx = conjuncts_sidx[conjunct]
							self.makeup_server_result(query_server_results, conjunct_sidx, self.unifed_sid_name, \
										executed_server, makeup_sidx, query_attrs)

			if len(delete_dict)>0:
				print("@deleting ....")
				for executed_server in executed_servers:
					if executed_server in query_server_results:
						print(f"@delete result in .... {executed_server}")
						sidx_name = server_related_sidx[executed_server]
						for conjunct, delete_items in delete_dict.items():
							sidx = [item[sidx_name] for item in delete_items]
							self.delete_server_result(query_server_results, executed_server, sidx)


				
			executed_servers.append(server)
			self.update_queue_score(conjuncts_value, squeue)
			print("@ending.... server")


		### get final results:

		## get final DNF sid_structure
		unifed_sid_dict = self.get_combined_dict(conjuncts_sidx, conjuncts_value, self.unifed_sid_name)

		if len(unifed_sid_dict) == 0:
			return []

		## handle attr_servers that are not in query_servers
		sum_id_str = ""
		sidx_items = unifed_sid_dict.values()

		for attr_server, attrs in server_require_attrs.items():

			if attr_server not in query_servers:
				print(f"\n#End: There is additional server {attr_server} need to...")
				sidx_name = server_related_sidx[attr_server]

				query_attrs = self.__get_server_query_attrs_str(attrs)
				sum_id_str = ", ".join(str(item[sidx_name]) for item in sidx_items )

				self.__restricted_query_and_modify_server_result(sum_id_str, attr_server, \
							query_attrs,  query_server_results)

		return self.connect_result(sidx_items, query_server_results, server_related_sidx )








							


				
				













