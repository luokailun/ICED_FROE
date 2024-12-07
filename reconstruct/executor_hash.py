

from executor import BasicExecutor, QueryLeastExecutor


class BasicHashExecutor(BasicExecutor):


	def __init__(self, data, relation_table_name, unifed_sid_name, DNF_structure,connection, hash_structure, hash_function):
		
		super().__init__(data, relation_table_name, unifed_sid_name, DNF_structure,connection)
		self.hash_structure = hash_structure
		self.hash_function = hash_function


	def __generate_sid_DNF(self, relation_table_name, unifed_sid_name):

	    cur = self.connection.cursor()

	    subsid_sqls_DNF = self.DNF_structure.generate_subsid_sql()
	    subsid_DNF = self.execute_DNF_sql(subsid_sqls_DNF, cur)
	    ###
	    ### change this
	    sid_sqls_DNF = self.DNF_structure.generate_unified_sid_sql(subsid_DNF, relation_table_name, unifed_sid_name)
	    sid_DNF = self.execute_DNF_sql(sid_sqls_DNF, cur)
	    
	    return sid_DNF




class QueryLeastHashExecutor(QueryLeastExecutor):


	def __init__(self, data, relation_table_name, unifed_sid_name, DNF_structure,connection, hash_structure, hash_function):
		
		super().__init__(data, relation_table_name, unifed_sid_name, DNF_structure,connection)
		self.hash_structure = hash_structure
		self.hash_function = hash_function











