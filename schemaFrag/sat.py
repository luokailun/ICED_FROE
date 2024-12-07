# -*- coding: utf-8 -*-
#!/usr/bin/python3
import os
import itertools
import pandas as pd
from sympy import *
from sympy.logic.inference import satisfiable
import random as rd
import time
import sys

# Calculate all cases where a projection operation on the specified set of servers results in an inferred set, return a list
# servers: list of servers
# inference: Inference set
# servers_required: list of client requirements

# Extrapolation of non-assignable cases from the set of extrapolations
def get_not_allow_list(servers, inference: list):
    not_allow_list = []

    for i in range(len(servers)):
        not_allow_list.append(Symbol(str(inference[0]) + ':' + str(servers[i])))

    for i in range(1, len(inference)):
        n = len(not_allow_list)
        for j in range(n):
            for k in range(len(servers)):
                not_allow_list.append(not_allow_list[0] & Symbol(str(inference[i]) + ':' + str(servers[k])))
            del not_allow_list[0]
    
    return not_allow_list

# The computational burden of not_allow_list is greatly reduced by the or operation.
def get_not_allow_list2(servers, inference: list):
    not_allow_list = []
    not_allow_expr = true
    for i in range(len(inference)):
        temp = false
        for j in range(len(servers)):
            temp |= Symbol(str(inference[i]) + ':' + str(servers[j]))
        not_allow_expr &= temp
    not_allow_list.append(not_allow_expr)

    return not_allow_list

# Extrapolation of mandatory allocations from the list of client requirements
def get_required_list(properties: list, servers_required: set):
    required_list = []
    server_id = 0
    for server_required in servers_required:
        server_query = true
        properties_set = set(properties)
        # Calculate the circumstances in which the allocation must be made
        for it in server_required:
            properties_set.remove(it)
            server_query &= Symbol(str(it) + ':' + str(server_id))
        # Remove redundant attribute assignments
        required_list.append(server_query)
        server_id += 1
    return required_list


# Convert problems to SAT questions and calculate attribute assignment schemes
def SAT(k, n, properties, sensitive_set: set, inferences: set, servers_required: set):

    # Adding sensitive attribute sets to the L-inference set
    inf = set()
    for it in inferences:
        inf.add(frozenset(set(it)))
    
    # Add user requirement sets to the L-inference set
    req = set()
    for it in servers_required:
        req.add(frozenset(set(it)))
    
    # Define all conditions: 
    s = []
    for it in properties:
        tlist = []
        # Constraint 3: To ensure the reliability of attribute separation, no basic sensitive attribute can appear in the assignment of any client i.
        if it in sensitive_set:
            continue
        for j in range(n):
            tlist.append(Symbol(str(it) + ':' + str(j)))
        s.append(tlist)

    expr_list = []
    
    # Constraint 1: To ensure the completeness of attribute separation, each attribute in B∗ must be assigned to at least one client
    property_closure = false
    for it in s:
        n_false_closure = true
        for i in range(n):
            n_false_closure &= ~it[i]
        property_closure |= n_false_closure
    property_closure = ~property_closure
    print('FA:',property_closure)
    expr_list.append(property_closure)
    del property_closure

    # Constraint 2: In order to ensure the reliability of attribute separation, every attribute needed by client i must be assigned to client i.
    tlist = get_required_list(properties, req)
    required_Allocate = true
    for exprt in tlist:
        required_Allocate &= exprt
    print('FR:',required_Allocate)
    expr_list.append(required_Allocate)
    del required_Allocate
    # Constraint 4: Achieving K-Security
    # Negate all cases where projections less than or equal to the k-conspiracy server can form an inference set
    inference_expr = true
    for inference in inf:
        for servers in itertools.combinations(range(n), k):
            tlist = get_not_allow_list2(servers, list(inference))
            # print(len(tlist))
            for exprt in tlist:
                inference_expr &= ~exprt
    print('FN,K:',inference_expr)
    expr_list.append(inference_expr)
    del inference_expr

    # synthetic ensemble paradigm
    expr = 0
    for i in range(len(expr_list)):
        # print(expr_list[i])
        if i == 0:
            expr = expr_list[i]
        else:
            expr &= expr_list[i]
    
    print("expr:", expr)
    sat = satisfiable(expr)
    return sat, expr

# Load the dataset
def import_data(file_path : str):
    # If you encounter data without a header, there may be some problems with the extracted attribute list, but for the time being, the default dataset has a header.
    data = pd.read_csv(file_path, delimiter=",")
    return data

# Randomly generate inference sets
# data: data set
# servers_required: set of server requirements
# inferences: inferred set
# sensitive_set: sensitive attribute set
def random_conditions_generated(data, k = 1, max_server_count : int = 10):
    result = true
    columns = list(data)
    sensitive_set = []
    
    # Randomly generated inferences
    inferences = set()
    # random param here:
    infCount = rd.randrange(1,6)
    for i in range(infCount):
        maxSize = 10
        minSize = min(k, maxSize-1)
        SingleInfSize = rd.randrange(minSize, maxSize, 1)
        sample = rd.sample(columns, SingleInfSize)
        inferences.add(frozenset(sample))
    print("inferences:", inferences)

    # 2 randomly generated servers_required 
    servers_required_temp = list()
    j = 0
    i = 0
    # 2.1 Segmentation of the attribute list to generate a server requirement set
    while i < len(columns) and len(servers_required_temp) < max_server_count-1:
        # random param here:
        i += rd.randrange(1, min(7, len(columns)), 1)
        i = min(len(columns), i)
        servers_required_temp.append(list(columns[j:i]))
        j = i
    if i < len(columns):
        servers_required_temp.append(list(columns[j:]))
    
    server_count = len(servers_required_temp)
    # 2.2 Switch elements between servers
    # random param here:
    switchTimes = rd.randrange(1, 10, 1)
    for it in range(switchTimes):
        shuffledA = rd.randrange(0, server_count, 1)
        shuffledB = rd.randrange(0, server_count, 1)
        lenA = len(servers_required_temp[shuffledA])
        lenB = len(servers_required_temp[shuffledB])
        if shuffledA == shuffledB:
            continue
        switchSize = rd.randrange(0, min(lenA, lenB), 1)
        if switchSize == 0:
            continue
        switchAElems = rd.sample(range(lenA), switchSize)
        switchBElems = rd.sample(range(lenB), switchSize)
        # swap
        for j in range(switchSize):
            temp = servers_required_temp[shuffledA][switchAElems[j]]
            servers_required_temp[shuffledA][switchAElems[j]] = servers_required_temp[shuffledB][switchBElems[j]]
            servers_required_temp[shuffledB][switchBElems[j]] = temp

    # 2.3 Randomly insert some attributes into the server requirements set that are not currently available on the server
    for it in range(server_count):
        insertElements = []
        # random param here:
        while rd.randrange(0, 2, 1) == 1:
            # Randomly select a server
            insertIndex = rd.randrange(0, len(servers_required_temp), 1)
            if insertIndex == it:
                continue
            # Randomly sample the internal elements of the server to get the new elements to be inserted.
            insertElements += rd.sample(servers_required_temp[insertIndex], rd.randrange(0, len(servers_required_temp[insertIndex]), 1))
        # Removing Duplicate Elements
        servers_required_temp[it] = list(set(insertElements + servers_required_temp[it]))
    

    # 2.4 Convert to a Set for subsequent operations
    servers_required = set()
    for it in servers_required_temp:
        servers_required.add(frozenset(it))
    # 2.5 Add redundant servers until max_server_count is met.
    while len(servers_required) < max_server_count:
        # Minimum 1 element required
        servers_required.add(frozenset(rd.sample(columns, rd.randrange(1, len(columns), 1))))
    server_count = len(servers_required)

    # print(len(servers_required))
    return result, servers_required, inferences, sensitive_set

# Calling the SAT solver for the solution
def kSaftyUsingSAT(data, k = 1, max_server_count : int = 10):
    getGenerated, servers_required, inferences, sensitive_set = random_conditions_generated(data, k = k, max_server_count=max_server_count)
    print("servers_required:",servers_required)
    if getGenerated == False:
        return []
    n = len(servers_required)
    # for k in range(1, n):
    

    start_time = time.time()
    sat, expr = SAT(k, n, list(data), sensitive_set, inferences, servers_required)
    end_time = time.time()
    time_cost = end_time - start_time
    # print(data.head())
    print("n=", n, "k=", k)
    print("properties(", len(list(data)), ")",list(data)) 
    print("sensitive sets: ", sensitive_set)
    print("inferences: ", inferences)
    print("server_required: ", servers_required)
    print("result: ", sat)
    print(f"time cost:{time_cost:.2f}s")

    trueSAT = []
    if type(sat) != type(True):
        for it in sat:
            if sat[it] == True:
                trueSAT.append(it)
                pass   
    else:
        print("no solution!")
        pass
    # Final distribution results
    if len(trueSAT) != 0:
        print("Final Allocate Result:",trueSAT) 
    return trueSAT, time_cost


# Generate various possible attribute assignment results based on SAT results (may be changed to minimum storage loss attribute assignment results later)
#trueSAT: string, SAT result, the first half is the attribute name, the second half is the server ID to which the attribute is assigned
#combine all possible server IDs to which the attribute can be assigned to generate all possible attribute assignment results
def generateAllPossiblePropertiesAllocationResult(properties: list, trueSAT: list):
    # Generate all possible attribute assignment results
    allPossiblePropertiesAllocationResult = []
    for it in trueSAT:
        currentResult = []
        property = it.split(":")[0]
        serverID = it.split(":")[1]
        for i in range(len(properties)):
            if properties[i] != property:
                currentResult.append(properties[i] + ":" + serverID)
                pass
        allPossiblePropertiesAllocationResult.append(currentResult)
    pass

class Logger:
    def __init__(self, filename='default.log', add_flag=True, stream=sys.stdout):
        self.terminal = stream
        self.filename = filename
        self.add_flag = add_flag

    def write(self, message):
        if self.add_flag:
            with open(self.filename, 'a+') as log:
                self.terminal.write(message)
                log.write(message)
        else:
            with open(self.filename, 'w') as log:
                self.terminal.write(message)
                log.write(message)

    def flush(self):
        pass
def exp1(n:list = [10,6,4,6,8,10,10,10], start_offset = 0, end_data_position = -1, file_folders = "reals/", k:list = [2,3], repeats = 10):
    solution = 0 # Number of repetitions of the complete experiment

    file_root = "datasets/" + file_folders
    getAllFiles = os.listdir(file_root)
    print("files:",getAllFiles)
    for filei in range(len(getAllFiles)):
        if filei < start_offset:
            continue
        if end_data_position > start_offset and filei > end_data_position:
            break
        file = getAllFiles[filei]
        avgTime = 0
        for ki in k:
            loggerFolder = "output/" + file_folders
            loggerFile = loggerFolder + "result-" + str(filei) + "-" + str(ki) + "-"+ str(int(time.time())) + ".txt"
            if not os.path.exists(loggerFolder):
                os.makedirs(loggerFolder)
            sys.stdout = Logger(loggerFile, sys.stdout)
            print("file:",file_root + file)
            print("repeatTimes:",repeats)
            for repeat in range(repeats):
                print("k=", ki, ",times=", repeat + 1)
                data = import_data(str(file_root + file))
                satResult, time_cost = kSaftyUsingSAT(data, ki, n[filei])
                avgTime += time_cost
            avgTime /= repeats
            print("repeatAvgTime:", avgTime, "[k=", ki, "]")
        filei += 1
    pass

def exp2(folder_name = "kValue", n = 6):
    file_root = "datasets/" + folder_name + "/"
    getAllFiles = os.listdir(file_root)
    print("files:",getAllFiles)
    for file in getAllFiles:    
        loggerFile = "output/" + folder_name + "/result-" + str(int(time.time())) + ".txt"
        sys.stdout = Logger(loggerFile, sys.stdout)
        print("file:",file_root + file)
        for i in range(3):
            print("times:", i+1)
            data = import_data(str(file_root + file))
            for k in range(1, n):
                # Generating Basic Security Constraint Assignment Possibilities
                print("k:",k)
                satResult = kSaftyUsingSAT(data, k, n)
    pass

if __name__ == "__main__":
    # exp1(start_offset=7, k = [3])
    exp1(start_offset=4, k=[4])
    # exp1()

    # exp2(n=5)
    # exp2(folder_name="kValue2")
        

