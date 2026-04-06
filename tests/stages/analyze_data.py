from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

def analyze_data(calibrated_arr : np.ndarray, factor: float) -> (None, None):

    savepath = Path('figs')
    savepath.mkdir(exist_ok=True, parents=True)
    fig, ax = plt.subplots()
    ax.imshow(calibrated_arr, cmap="viridis")
    ax.set_title("Calibrated Data Heatmap")
    fig.savefig(savepath / "calibrated_data_heatmap.png", bbox_inches='tight')

    return None
