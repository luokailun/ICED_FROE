import heapq

class ScoreQueue:

    def __init__(self):
        self.heap = []
        self.index = 0  # 用于稳定排序

    def push(self, item, score):
        # 使用负数来实现最大堆，因为 Python 默认是最小堆
        heapq.heappush(self.heap, (-score, self.index, item))
        self.index += 1

    def pop(self):
        if not self.heap:
            raise IndexError("pop from an empty queue")
        # 弹出最大值
        score, _, item = heapq.heappop(self.heap)
        return item, -score  # 返回正的 score

    def update_scores(self, update_func, update_elemet):
        # 使用 update_func 对每个元素的 score 进行更新
        new_heap = []
        while self.heap:
            score, index, item = heapq.heappop(self.heap)
            new_score = update_func(item, update_elemet)  # 更新时传入原 score 和 update_elemet
            heapq.heappush(new_heap, (-new_score, index, item))
        self.heap = new_heap

    def is_empty(self):
        return len(self.heap) == 0



class Score(object):
    """docstring for Score"""

    try:
        with open('data.txt', 'r') as file:
            score_dict = file.read().strip()
    except FileNotFoundError:
            score_dict = dict()
    except Exception as e:
            score_dict = f"Error: {e}"

    @staticmethod
    def get_score(pred):
        if pred in Score.score_dict:
            return Score.score_dict[pred]
        else:
            for sym in pred.split(" "):
                if sym.strip() in (">", ">=", "<", "<="):
                    return 100000
                elif sym.strip() in ("="):
                    return 10

            print("get_score: ERROR for the symbol %s"%sym)
            exit(0)
                    
             

        


# 示例使用
# queue = ScoreQueue()
# queue.push("element1", 10)
# queue.push("element2", 20)
# queue.push("element3", 15)

# print("最大值出列:", queue.pop())  # 应输出 element2, 20

# # 更新其他元素的 score，例如减去一个常数
# queue.update_scores(lambda score, item: score - 1)

# # 检查更新后的队列
# print("更新后的最大值出列:", queue.pop())  # 应输出 element3, 更新后的分数

# print(not queue.is_empty())
