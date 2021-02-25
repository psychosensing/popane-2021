import multiprocessing as mp
import os
import pandas as pd
import re
import tqdm
import traceback


dir_path = 'data/Study 7'
baseline_marker_id = '-1'

# condition.xlsx
co = pd.read_excel('data/condition5.xlsx', 'study7')

st = pd.read_excel('data/condition6.xlsx', 'list of stimuli')

# missing.xlsx
mi = pd.read_excel('data/missing.xlsx', 'Study7')

dzdt_re_pattern = re.compile('(0*\d+)_.*')


def process_path(dzdt_path):
    try:
        subject_id_3digit = re.findall(dzdt_re_pattern, dzdt_path)[0]
        subject_id = int(subject_id_3digit.lstrip('0'))

        # condition.xlsx
        subject_row = co.loc[co['id'] == subject_id]

        try:
            subject_age = int(subject_row['wiek'])
        except Exception:
            subject_age = 0

        subject_sex = int(subject_row['sex'])

        try:
            film1_marker_id = str(int(subject_row['film1']))
            film2_marker_id = str(int(subject_row['film2']))
            film3_marker_id = str(int(subject_row['film3']))
            film4_marker_id = str(int(subject_row['film4']))
            film5_marker_id = str(int(subject_row['film5']))
        except Exception as e:
            print(f'{dzdt_path} - some film marker does not exist for this subject, processing skipped.')
            return 0

        film1_marker_name = st[st.iloc[:, 2] == int(film1_marker_id)].iloc[:, 4].item()
        film2_marker_name = st[st.iloc[:, 2] == int(film2_marker_id)].iloc[:, 4].item()
        film3_marker_name = st[st.iloc[:, 2] == int(film3_marker_id)].iloc[:, 4].item()
        film4_marker_name = st[st.iloc[:, 2] == int(film4_marker_id)].iloc[:, 4].item()
        film5_marker_name = st[st.iloc[:, 2] == int(film5_marker_id)].iloc[:, 4].item()

        # load dz, dzdt, ecg, z0
        dz_path = dzdt_path.replace('DZDT', 'DZ')
        ecg_path = dzdt_path.replace('DZDT', 'ECG')
        z0_path = dzdt_path.replace('DZDT', 'Z0')

        for path in [dzdt_path, dz_path, ecg_path, z0_path]:
            if not os.path.exists(path):
                print(f'{path} does not exist, processing skipped.')
                return 0

        dz = pd.read_csv(dz_path, sep=' ', header=None, skiprows=3, names=['sample_id', 'dz', 'dz_peak', 'marker_dz', 'val4'])
        dz.drop(columns='val4', inplace=True)

        dz_markers = list(dz['marker_dz'].unique())
        for m in [1, 11, 21, 31, 41, 51]:
            if m not in dz_markers:
                print(f'{dz_path} - no marker {m} in file, processing skipped.')
                return 0

        dzdt = pd.read_csv(dzdt_path, sep=' ', header=None, skiprows=3, names=['sample_id', 'dzdt', 'val2', 'val3', 'val4'])
        ecg = pd.read_csv(ecg_path, sep=' ', header=None, skiprows=3, names=['sample_id', 'ecg', 'val2', 'val3', 'val4'])
        z0 = pd.read_csv(z0_path, sep=' ', header=None, skiprows=3, names=['sample_id', 'z0', 'val2', 'val3', 'val4'])

        # merge dataframes
        df = pd.merge(dz, dzdt[['sample_id', 'dzdt']], on='sample_id', how='inner')
        df = pd.merge(df, ecg[['sample_id', 'ecg']], on='sample_id', how='inner')
        df = pd.merge(df, z0[['sample_id', 'z0']], on='sample_id', how='left')
        del dz, dzdt, ecg, z0

        # interpolate z0
        df = df.fillna(method='ffill')
        df = df.fillna(method='bfill')

        # read hafee
        hafee_path = f'data/Study 7/HafeeNCN_{subject_id_3digit}b.txt'
        HF = os.path.exists(hafee_path)

        if HF:
            column_names = ['timestamp', 'meter', 'marker_hafee']
            dtype_dict = {k: 'float' for k in column_names}
            dtype_dict['marker_hafee'] = 'string'

            hf = pd.read_csv(hafee_path, sep='\t', decimal=',', header=None, skiprows=9, names=column_names, dtype=dtype_dict)
            hf['marker_hafee'] = hf['marker_hafee'].apply(lambda x: x.strip() if isinstance(x, str) else x)

            HF_11 = hf['marker_hafee'].isin(['#* 11']).any()
            if HF_11:
                # find synchronization point
                df_sync_sample_id = df.index[df['marker_dz'] == 11][0]
                hf_sync_timestamp_ms = hf.index[hf['marker_hafee'] == '#* 11'][0]

                rows_diff = df_sync_sample_id - hf_sync_timestamp_ms

                if rows_diff > 0:  # if hafee recording started LATER
                    df = df.iloc[rows_diff:]
                    df = df.reset_index(drop=True)
                else:
                    hf = hf.iloc[-rows_diff:]
                    hf = hf.reset_index(drop=True)

            else:
                # hafee_markers = list(hf['marker_hafee'].unique())
                print(f'{hafee_path} - no sync marker "#* 11" in file, fill meter with nan.')
                hf = pd.DataFrame(index=df.index, columns=['timestamp', 'meter', 'marker_hafee'])
                hf = hf.fillna(value=float('nan'))

        else:
            print(f'{hafee_path} does not exist, fill meter with nan.')
            hf = pd.DataFrame(index=df.index, columns=['timestamp', 'meter', 'marker_hafee'])
            hf = hf.fillna(value=float('nan'))

        # merge hafee
        df = df.merge(hf, how='inner', left_index=True, right_index=True)
        del hf

        df['timestamp'] = pd.timedelta_range(start=0, periods=len(df), freq='ms').total_seconds()

        df = df[['timestamp', 'meter', 'ecg', 'dzdt', 'dz', 'z0', 'marker_dz', 'marker_hafee']].copy()
        df['marker'] = ''

        df_idx_base = df.index[df['marker_dz'] == 1][0] - 60000
        df.loc[df_idx_base:df_idx_base + 300000, 'marker'] = baseline_marker_id

        df_idx_film1 = df.index[df['marker_dz'] == 11][0]
        df.loc[df_idx_film1:df_idx_film1 + 120000, 'marker'] = film1_marker_id

        df_idx_film2 = df.index[df['marker_dz'] == 21][0]
        df.loc[df_idx_film2:df_idx_film2 + 120000, 'marker'] = film2_marker_id

        df_idx_film3 = df.index[df['marker_dz'] == 31][0]
        df.loc[df_idx_film3:df_idx_film3 + 120000, 'marker'] = film3_marker_id

        df_idx_film4 = df.index[df['marker_dz'] == 41][0]
        df.loc[df_idx_film4:df_idx_film4 + 120000, 'marker'] = film4_marker_id

        df_idx_film5 = df.index[df['marker_dz'] == 51][0]
        df.loc[df_idx_film5:df_idx_film5 + 120000, 'marker'] = film5_marker_id

        df = df.drop(columns=['marker_dz', 'marker_hafee'])
        df = df.rename(columns={'ecg': 'ECG', 'meter': 'affect'})

        for c in ['timestamp']:
            df.loc[:, c] = df[c].map(lambda x: '%.10g' % x)

        for c in ['ECG', 'dz', 'dzdt', 'z0']:
            if df[c].isnull().all():
                continue

            df.loc[:, c] = df[c].map(lambda x: '%.3f' % x)

        header = f"""#Study_name,Study 7
#Subject_ID,{subject_id}
#Subject_Age,{subject_age}
#Subject_Sex,{subject_sex}
#Channel_Name,timestamp,affect,ECG,dzdt,dz,z0,marker
#Data_Category,timestamp,data,data,data,data,data,marker
#Data_Unit,second,millivolts,millivolts,ohm/s,ohm,ohm,string
#Data_Sample_rate,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz
#Data_Device,LabChart 8.19 (ADInsturments, New Zealand),Response Meter (ADInsturments, New Zealand),ECG (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),ICG  (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),ICG  (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),ICG  (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),Labchart 8.19 (AdInsturments, New Zealand)
"""

        # to .csv
        df_base = df[df_idx_base:df_idx_base + 300000]
        with open(f'{subject_id}_Baseline.csv', 'w', newline='') as f:
            f.write(header)
            df_base.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_film1 = df[df_idx_film1:df_idx_film1 + 120000]
        with open(f'{subject_id}_{film1_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film1.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_film2 = df[df_idx_film2:df_idx_film2 + 120000]
        with open(f'{subject_id}_{film2_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film2.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_film3 = df[df_idx_film3:df_idx_film3 + 120000]
        with open(f'{subject_id}_{film3_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film3.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_film4 = df[df_idx_film4:df_idx_film4 + 120000]
        with open(f'{subject_id}_{film4_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film4.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_film5 = df[df_idx_film5:df_idx_film5 + 120000]
        with open(f'{subject_id}_{film5_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film5.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        with open(f'{subject_id}_All.csv', 'w', newline='') as f:
            f.write(header)
            df.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

    except Exception as e:
        print(dzdt_path, '\n', repr(e), '\n', traceback.format_exc())


if __name__ == '__main__':
    pool = mp.Pool(5)
    dzdt_paths = [os.path.join(dir_path, p) for p in os.listdir(dir_path) if '_DZDT.txt' in p]
    r = list(tqdm.tqdm(pool.imap(process_path, dzdt_paths), total=len(dzdt_paths)))
