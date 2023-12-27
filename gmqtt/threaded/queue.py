import logging

import queue


logger = logging.getLogger(__name__)

class SizeLimitedQueue(queue.Queue):
    def __init__(self, max_size = 200):
        super().__init__()
        self.max_size = max_size

    def get_nowait(self):
        if self.qsize() > self.max_size:
            qsize = self.qsize()
            logging.warning(f"QUEUE BIGGER THAN {self.max_size}: {qsize}")

            # raise ValueError()

            for i in range(qsize - int(0.9 * self.max_size)):
                if self.qsize() > 1:
                    self.get_nowait()

            logging.warning("DROPPED {} MESSAGES", qsize - self.qsize())

        return self.get_nowait()
