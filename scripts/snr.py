import numpy as np
import os
import pandas as pd
import scipy.interpolate
import scipy.signal


def get_snr(csv_path, signal_name):
    df = pd.read_csv(csv_path, skiprows=11)
    sn = df[signal_name].to_numpy()

    acorrsn = scipy.signal.correlate(sn, sn, 'full')

    mu = np.mean(sn)

    n0 = len(sn)
    offset = 2

    x = list(range(n0 - offset - 1, n0 + offset, 1))
    y = list(acorrsn[x])

    x0 = x.pop(offset)
    y0 = y.pop(offset)

    # interpolate by polynomial
    f_p2 = np.polyfit(x, y, 2)
    y0_int = np.polyval(f_p2, x0)

    snr_est = 10 * np.log10((y0_int - mu ** 2) / (y0 - y0_int))

    return snr_est


if __name__ == '__main__':
    root_path = 'data/study1'  # Set your path to data directory here

    with open('snr_summary.csv', 'w+') as f:
        f.write('Study,Subject_ID,Stimuli,Signal,SNR\n')

        for root, dir_names, file_names in os.walk(root_path):
            if not dir_names:
                study_name = os.path.basename(root)
                for file_name in file_names:
                    if 'Baseline' not in file_name and 'All' not in file_name:
                        subject_name = file_name.split('.')[0]
                        subject_id, subject_stimuli = subject_name.split('_', maxsplit=1)
                        file_path = '/'.join([root, file_name])

                        for signal_name in ['EDA', 'SBP', 'DBP', 'CO', 'TPR', 'temp', 'respiration', 'ECG', 'dzdt', 'dz', 'z0']:
                            try:
                                snr = get_snr(file_path, signal_name)

                                msg = f'{study_name},{subject_id},{subject_stimuli},{signal_name},{snr:.3f}'
                                print(msg)
                                f.write(msg + '\n')
                            except Exception as e:
                                pass
