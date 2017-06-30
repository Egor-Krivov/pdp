import numpy as np

MBs = 10000
size = MBs // 4
global_array = np.ones((size, 1024, 1024), dtype=np.float32)
