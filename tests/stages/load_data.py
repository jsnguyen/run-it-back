import numpy as np

def load_data(name : str = 'not test') -> (np.ndarray, float):
    print(name)
    rng = np.random.default_rng(seed=42)
    arr = rng.standard_normal((100,100))
    return arr, 100
