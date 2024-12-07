
import pandas as pd
from number import ExtendedInfinity

class Condition:

    def __init__(self, parts):
        self.parts = tuple(parts)
        self.hash = hash((self.__class__, self.parts))
    def __hash__(self):
        return self.hash
    def dump(self, indent="  "):
        print("%s%s" % (indent, self._dump()))
        for part in self.parts:
            part.dump(indent + "  ")
    def _dump(self):
        return self.__class__.__name__      
    def __eq__(self, other):
        # Compare hash first for speed reasons.
        return (self.hash == other.hash and
                self.__class__ is other.__class__ and
                self.parts == other.parts)



class Conjunction(Condition):

    def __init__(self, preds, attr2server_dict):
        self.preds = tuple(preds)
        self.hash = hash((self.__class__, self.preds))
        self.server2preds = dict()
        self.pred2server = dict()
        self.__construct_server_preds_rel(attr2server_dict)
        self.servers = self.server2preds.keys()


    def __construct_server_preds_rel(self, attr2server_dict):

        self.server2preds = {server: list() for server in attr2server_dict.values() \
                    if server.find('_relation')==-1 }
        for pred in self.preds:
            for sym in str(pred).split(' '):
                if str(sym) in attr2server_dict:
                    self.server2preds[attr2server_dict[sym]].append(pred)
                    self.pred2server[pred] = attr2server_dict[sym]

    def get_pred_score(self):
        from score import Score
        return { pred: Score.get_score(pred) for pred in self.preds}



    def get_requried_query_servers(self):
        servers = list()
        for server, preds in self.server2preds.items():
            if len(preds)>0:
                servers.append(server)

        return servers

    def get_server_preds(self, server):
        return self.server2preds[server]




    def dump(self, indent="  "):
        print("%s%s" % (indent, self._dump()))
        for part in self.preds:
            print(indent + "  ", part)


    def generate_subsid_sql(self):
        subsid_sql_list = list()

        for server in self.servers:
            #保持一致
            preds = self.server2preds[server]
            if preds!=list():
                where_condition = ' AND '.join([ pred for pred in preds])
                subsid_sql_list.append(f"SELECT sid FROM {server} WHERE {where_condition}")

        return subsid_sql_list


    def generate_unified_sid_sql(self, subsid_list, relation_table_name, unifed_sid_name, relatesidx):
        sql_list =list()
        e = 0
        for server in self.servers:
            #保持一致
            preds = self.server2preds[server]
            if preds!=list():
                sid_list_str = ', '.join(map(str, subsid_list[e]['sid'].tolist()))
                e +=1
                if sid_list_str != 'None':
                    sql = f"SELECT {unifed_sid_name} FROM {relation_table_name} WHERE {relatesidx[server]} IN ({sid_list_str})"
                    sql_list.append(sql)
                else:
                    #sql = f"SELECT {unifed_sid_name} FROM {relation_table_name} WHERE {relatesidx[server]} = -1"
                    return [None]
        return sql_list



class Disjunction(Condition):
    

    def __init__(self, conjuncts, stbmap, relatesidx):

        self.parts = list(conjuncts)
        self.server_requried_attrs = dict()
        for server, (attrs, _) in stbmap.items():
            if len(attrs)>0:
                self.server_requried_attrs[server] = attrs

        self.server_relatesidx = dict(relatesidx)
        # servers needed to obtain attributes for the final result
        self.requried_attr_servers = self.__get_requried_attr_servers() 
        self.requried_query_servers = self.__get_requried_query_servers()

        self.server_conjunct_preds = self.__get_server_conjunct_preds()
        self.pred_score = self.__get_pred_score()


    def __get_requried_attr_servers(self):

        server_list = list()
        for server, attrs in self.server_requried_attrs.items():
            if(len(attrs))>0:
                server_list.append(server)
        return server_list

    def get_server_required_attrs(self):
        return self.server_requried_attrs
        

    def __get_requried_query_servers(self):

        servers = set()
        for part in self.parts:
            servers |= set(part.get_requried_query_servers())

        return servers

    def __get_server_conjunct_preds(self):
        mdict = dict()
        for server in self.requried_query_servers:
            mdict[server] = dict()
            for part in self.parts:
                mdict[server][part] = part.get_server_preds(server)
        return mdict

    def __get_pred_score(self):
        mdcit = dict()
        for part in self.parts:
            mdcit.update(part.get_pred_score())
        return mdcit

    def get_server_conjunct_preds(self):
        return self.server_conjunct_preds

    def get_query_servers(self):
        return self.requried_query_servers

    def get_conjuncts(self):
        return self.parts

    def get_server_relatedsidx(self):
        return self.server_relatesidx

    def generate_subsid_sql(self):
        
        return [ part.generate_subsid_sql() for part in self.parts ]


    def generate_unified_sid_sql(self, subsid_DNF, relation_table_name, unifed_sid_name):
        
        sqls_list = list()

        for e, part in enumerate(self.parts):
            #print(subsid_DNF[e])
            sqls = part.generate_unified_sid_sql(subsid_DNF[e], relation_table_name, unifed_sid_name, self.server_relatesidx)
            sqls_list.append(sqls)
        return sqls_list


    def generate_server_required_sqls(self, result_sids):

        sql_list = list()

        for server in self.requried_attr_servers:

            attrs = self.server_requried_attrs[server]
            sub_query_fields = ", ".join(attrs)
            subsids = [str(item[self.server_relatesidx[server]]) for item in result_sids]
            r_list_str = ", ".join(subsids)
            if r_list_str != 'None':
                sql = f"SELECT sid, {sub_query_fields} FROM {server} WHERE sid IN ({r_list_str})"
            else:
                sql = f"SELECT sid, {sub_query_fields} FROM {server} WHERE sid = -1"

            sql_list.append(sql)

        return sql_list


    def connect_result(self, full_sids, data, results, bool_df):


        if not results:  # 如果没有结果
            return [] if not bool_df else pd.DataFrame()

        merged_df = pd.DataFrame(full_sids)  # 初始化 merged_df 为 full_sids
        for e, result in enumerate(results):
            server = self.requried_attr_servers[e]
            related_sid = self.server_relatesidx[server]
            #print(merged_df)
            #print(result)
            merged_df = pd.merge(merged_df, result, left_on=related_sid, right_on='sid',
                                       how="inner", suffixes=('', '_dump'))

        # 过滤列
        regstr = '^(?!.*_dump)(?!sid[0-9]+)'
        merged_df = merged_df.filter(regex=regstr)

        return merged_df if bool_df else merged_df.to_dict(orient='records')

    #############################################################################################################################
        


    def get_server_score(self, server, conjuncts_value):

        score = 0
        for conjunct, preds in self.server_conjunct_preds[server].items():
            if len(preds) == 0: 
                preds_score = ExtendedInfinity()
            else:
                preds_score = 0
                for pred in preds:
                    preds_score += self.pred_score[pred]
            # print(conjuncts_value[conjunct])
            # print(preds_score)
            # print(score)
            score = conjuncts_value[conjunct] - preds_score + score
        return score


        


    ## Think about where A or A and B, we will assume that it will not happen, 
    ## as it equivalent to A







