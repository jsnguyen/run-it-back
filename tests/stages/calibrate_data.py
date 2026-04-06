import numpy as np

def calibrate_data(arr : np.ndarray) -> np.ndarray:

    arr = arr / np.sum(arr)

    return arr

def alt_calibrate(arr : np.ndarray, mult: float) -> (np.ndarray, float):

    arr = arr / np.sum(arr) * mult

    return arr, 2.0
