from collections import OrderedDict
import time


class TwoQueue_Cache:

    # initialising capacity
    def __init__(self, capacity: int):
        self.lru_queue = OrderedDict()
        self.fifo_queue = OrderedDict()
        self.size_capacity = capacity
        self.fifo_size = 0
        self.lru_size = 0

    def get(self, key: str):
        if key not in self.lru_queue and key not in self.fifo_queue:
            return None
        else:
            pop_name = None
            if key in self.fifo_queue:
                pop_name = self.move_from_fifo2lru(key)
            self.lru_queue.move_to_end(key)
            self.lru_queue[key]['access_time'] += 1
            self.lru_queue[key]['access_time_stamp'] = time.time()
            return pop_name

    def move_from_fifo2lru(self, key):
        pop_name = None
        self.lru_queue[key] = self.fifo_queue[key]
        self.fifo_size -= self.fifo_queue[key]['size']
        self.lru_size += self.fifo_queue[key]['size']
        if self.lru_size > 0.5 * self.size_capacity:
            pop_name, _ = self.lru_queue.popitem(last=False)
        del self.fifo_queue[key]
        return pop_name

    def put(self, key: str, value):
        if key not in self.lru_queue and key not in self.fifo_queue:
            # 插入新值
            self.fifo_queue[key] = value
            self.fifo_queue.move_to_end(key)
            pop_name = None

            self.fifo_size += self.fifo_queue[key]['size']
            if self.fifo_size > 0.5 * self.size_capacity:
                pop_name, _ = self.fifo_queue.popitem(last=False)
            return pop_name
        else:
            if key in self.fifo_queue:
                # 更新已有值
                self.move_from_fifo2lru(key)
            else:
                self.lru_queue.move_to_end(key)
            self.lru_queue[key]['access_time'] += 1
            self.lru_queue[key]['access_time_stamp'] = time.time()
            return None