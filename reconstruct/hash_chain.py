from collections import defaultdict
import hashlib


from collections import defaultdict



# Function 1: Generate hash_chain dictionary from a hash chain string
def generate_hash_chain(chain_str, hash_function):
    """
    Generate a hash_chain dictionary from a hash chain string.

    Parameters:
    - chain_str: A string representing the hash chain (e.g., "center -> a -> b -> c -> center").
    - hash_function: A function to apply for hashing elements, except for the "center".

    Returns:
    - A dictionary where hash_chain[a][b] provides the hash path from element `a` to `b`.
    """
    elements = chain_str.split(" -> ")
    hash_chain = defaultdict(dict)

    # Traverse the elements to establish connections
    for i in range(len(elements)):
        for j in range(len(elements)):
            if elements[i]!='center' and elements[j]!='center':
                path = []
                if i==j:
                    path = []
                elif i < j:
                    # Forward path
                    path = elements[i+1:j+1]
                else:
                    # Backward path
                    path = elements[i+1:] + elements[:j+1]

                # Apply the hash_function or "center" to elements
                hashed_path = [hash_function if f != "center" else "center" for f in path]
                hash_chain[elements[i]][elements[j]] = hashed_path

    return hash_chain

# Example usage
def example_hash_function(value):
    """Example hash function using SHA-256."""
    import hashlib
    return hashlib.sha256(value.encode()).hexdigest()




import types




# Function 2: Compute TID of target server
def compute_tid(hash_chain, server_s, server_t, tids_s, query_func, hash_function, hash_server):
    """
    hash_chain: dict of {server: {other_server: [functions like 'hash' or 'center']}}
    server_s: the source server
    server_t: the target server
    tid_s: the TID of the source server
    query_func: a function to execute queries when 'center' is encountered
    """
    new_tids = tids_s

    # Get the dependency list for server_s to server_t
    operations = hash_chain.get(server_s, {}).get(server_t, [])
    for operation in operations:
        if isinstance(operation, types.FunctionType):
            new_tids = [operation(tid) for tid in new_tids ]
        elif operation == "center":
            # Identify the server connected to 'center'
            if hash_function is not None:
                new_tids = [hash_function(tid) for tid in new_tids ]


            # Query center for TID translation
            from
            query = f"SELECT TID FROM {hash_server} WHERE sid in '{new_tid}'"


    return new_tid


chain_str = "center -> a -> b -> c -> d"
hash_chain = generate_hash_chain(chain_str, example_hash_function)

# Print the hash chain dictionary
# for start, targets in hash_chain.items():
#     for end, path in targets.items():
#         print(f"{start} -> {end}: {path}")


# For Function 2
# hash_chain = {
#     "s": {"t": ["hash", "center", "hash"]},
#     "k": {"center": ["hash"]},
# }
tid_s = "example_tid"
query_func = lambda query: "new_tid_from_center"  # Mock query function
result_tid = compute_tid(hash_chain, "b", "a", tid_s, query_func, example_hash_function, "hash")
print("Computed TID:", result_tid)
