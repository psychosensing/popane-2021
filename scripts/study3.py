import collections
import multiprocessing as mp
import os
import pandas as pd
import re
import tqdm
import traceback

# missing.xlsx
mi = pd.read_excel('data/missing.xlsx', 'Study2')

# condition.xlsx
co = pd.read_excel('data/condition5.xlsx', 'study3')

st = pd.read_excel('data/condition6.xlsx', 'list of stimuli')

baseline_marker_id = '-1'

final_columns = ['timestamp', 'meter', 'ECG', 'EDA', 'dzdt', 'dz', 'z0', 'SBP', 'DBP', 'CO', 'TPR', 'marker']


def process_paths(paths):
    try:
        subject_id, dz_path, gr_path = paths
        DZ = dz_path is not None
        GR = gr_path is not None

        # missing.xlsx
        missing_value = mi.loc[mi['id'] == subject_id, 'channel'].item()

        if missing_value == 'all':
            return 0

        # condition.xlsx
        subject_sex = int(co.loc[co['id'] == subject_id, 'sex'].item())
        subject_age = int(co.loc[co['id'] == subject_id, 'wiek'].item())
        sms1_marker_id = int(co.loc[co['id'] == subject_id, 'sms1'].item())
        sms2_marker_id = int(co.loc[co['id'] == subject_id, 'sms2'].item())

        sms1_marker_name = st[st.iloc[:, 2] == sms1_marker_id].iloc[:, 4].item()
        sms2_marker_name = st[st.iloc[:, 2] == sms2_marker_id].iloc[:, 4].item()

        if DZ:
            # load dz, dzdt, ecg, z0
            dz = pd.read_csv(dz_path, sep=' ', header=None, skiprows=3, names=['sample_id', 'dz', 'marker_dz', 'val1', 'val2', 'val3', 'val4', 'val5', 'val6'], dtype={'marker_dz': 'string'})
            dzdt = pd.read_csv(dz_path.replace('DZ.txt', 'DZDT.txt'), sep=' ', header=None, skiprows=3, names=['sample_id', 'dzdt', 'marker_dzdt', 'val1', 'val2', 'val3', 'val4', 'val5', 'val6'])
            ecg = pd.read_csv(dz_path.replace('DZ.txt', 'ECG.txt'), sep=' ', header=None, skiprows=3, names=['sample_id', 'ecg', 'marker_ecg', 'val1', 'val2', 'val3', 'val4', 'val5', 'val6'])
            z0 = pd.read_csv(dz_path.replace('DZ.txt', 'Z0.txt'), sep=' ', header=None, skiprows=3, names=['sample_id', 'z0', 'marker_z0', 'val1', 'val2', 'val3', 'val4', 'val5', 'val6'])

            # drop obsolete column
            dz.drop(columns=['val1', 'val2', 'val3', 'val4', 'val5', 'val6'], inplace=True)
            dzdt.drop(columns=['val1', 'val2', 'val3', 'val4', 'val5', 'val6'], inplace=True)
            ecg.drop(columns=['val1', 'val2', 'val3', 'val4', 'val5', 'val6'], inplace=True)
            z0.drop(columns=['val1', 'val2', 'val3', 'val4', 'val5', 'val6'], inplace=True)

            # merge dataframes
            dz = pd.merge(dz, dzdt[['sample_id', 'dzdt']], on='sample_id', how='inner')
            dz = pd.merge(dz, ecg[['sample_id', 'ecg']], on='sample_id', how='inner')
            dz = pd.merge(dz, z0[['sample_id', 'z0']], on='sample_id', how='left')
            del dzdt, ecg, z0

            # replace marker_dz
            dz['marker_dz'] = dz['marker_dz'].replace('-9999', '')

            # interpolate z0
            dz['z0'] = dz['z0'].fillna(method='ffill')
            dz['z0'] = dz['z0'].fillna(method='bfill')

            # rename columns
            dz = dz.rename(columns={'ecg': 'ECG'})

            df = dz.copy()

        if GR:
            # load gratis
            column_names = ['timestamp', 'TPR', 'CO', 'meter', 'HR', 'SYS', 'DIA', 'GSR', 'marker_gr']
            dtype_dict = {k: 'float' for k in column_names}
            dtype_dict['marker_gr'] = 'string'

            with open(gr_path) as f:
                for i, line in enumerate(f):
                    if 'BottomValue' in line:
                        rows_to_skip = i + 1  # for files with multiple headers inside

            gr = pd.read_csv(gr_path, sep='\t', header=None, skiprows=rows_to_skip, encoding='cp1250', decimal=',', names=column_names, dtype=dtype_dict)
            gr['marker_gr'] = gr['marker_gr'].apply(lambda x: x.strip() if type(x) == str else x)
            gr.insert(0, 'sample_id', range(1, len(gr)+1))

            gr = gr.rename(columns={'GSR': 'EDA', 'SYS': 'SBP', 'DIA': 'DBP'})

        if DZ and GR:
            # synchronize
            dz_sync_idx = dz.index[dz['marker_dz'] == '1'][0]
            gr_sync_idx = gr.index[gr['marker_gr'] == '#* baseline1'][0]

            rows_diff = dz_sync_idx - gr_sync_idx
            assert rows_diff > 0  # assert, that gratis recording started LATER than other signals

            dz = dz.iloc[rows_diff:]
            gr['sample_id'] = [rows_diff + i + 1 for i in range(len(gr))]

            dz = dz.reset_index(drop=True)

            df = dz.merge(gr, on='sample_id', how='inner')

        elif not DZ and GR:
            df = gr.copy()

        for c in final_columns:
            if c not in df.columns:
                df[c] = float('nan')

        # missing.xlsx
        if missing_value == 'SBP,DBP,CO, TPR':
            for c in ['SBP', 'DBP', 'CO', 'TPR']:
                df[c] = float('nan')

        elif missing_value == 'ecg, dz, dz/dt, zo':
            for c in ['ECG', 'dz', 'dzdt', 'z0']:
                df[c] = float('nan')

        # add markers
        if GR:
            idx_base = df.index[df['marker_gr'] == '#* baseline1'][0]
            idx_sms1 = df.index[df['marker_gr'] == '#* min2'][0]
            idx_sms2 = df.index[df['marker_gr'] == '#* min3'][0]
        else:
            idx_base = df.index[df['marker_dz'] == '1'][0]
            idx_sms1 = df.index[df['marker_dz'] == '4'][0]
            idx_sms2 = df.index[df['marker_dz'] == '11'][0]

        df['marker'] = float('nan')
        df.loc[idx_base:idx_base+180000, 'marker'] = baseline_marker_id
        df.loc[idx_sms1:idx_sms1+180000, 'marker'] = sms1_marker_id
        df.loc[idx_sms2:idx_sms2+180000, 'marker'] = sms2_marker_id

        df['timestamp'] = pd.timedelta_range(start=0, periods=len(df), freq='ms').total_seconds()
        df = df[final_columns]
        df = df.rename(columns={'meter': 'affect'})

        for c in ['timestamp']:
            df.loc[:, c] = df[c].map(lambda x: '%.10g' % x)

        for c in ['ECG', 'dz', 'dzdt', 'z0']:
            df.loc[:, c] = df[c].map(lambda x: '%.3f' % x)

        # to csv
        header = f"""#Study_name,Study 3
#Subject_ID,{subject_id}
#Subject_Age,{subject_age}
#Subject_Sex,{subject_sex}
#Channel_Name,timestamp,affect,ECG,EDA,dz/dt,dz,z0,SBP,DBP,CO,TPR,marker
#Data_Category,timestamp,data,data,data,data,data,data,data,data,data,data,marker
#Data_Unit,second,custom,millivolts,microsiemens,Ohm/s,Ohm,Ohm,mmHg,mmHg,l/min,mmHg*min/l,string
#Data_Sample_rate,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,Beat-to-beat,Beat-to-beat,Beat-to-beat,Beat-to-beat,1000Hz
#Data_Device,LabChart 8.19 (ADInsturments, New Zealand),Response Meter (ADInsturments, New Zealand),ECG (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),GSR Amp (ADInstruments, New Zealand),ICG  (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),ICG  (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),ICG  (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Labchart 8.19 (AdInsturments, New Zealand)
"""
        df_base = df[idx_base:idx_base + 180000]
        with open(f'{subject_id}_Baseline.csv', 'w', newline='') as f:
            f.write(header)
            df_base.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_sms1 = df[idx_sms1:idx_sms1 + 180000]
        with open(f'{subject_id}_{sms1_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_sms1.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_sms2 = df[idx_sms2:idx_sms2 + 180000]
        with open(f'{subject_id}_{sms2_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_sms2.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        with open(f'{subject_id}_All.csv', 'w', newline='') as f:
            f.write(header)
            df.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

    except Exception as e:
        print(paths, '\n', repr(e), '\n', traceback.format_exc())


if __name__ == '__main__':
    pool = mp.Pool(8)

    dir_path = 'data/Study 3'

    # ids - dz, dzdt, ecg, z0
    dz_filenames = [p for p in os.listdir(dir_path) if '_DZ.txt' in p]
    dz_ids = [int(p.split('_')[0].lstrip('0')) for p in dz_filenames]

    # ids - gratis
    gr_filenames = [p for p in os.listdir(dir_path) if 'GRATIS' in p]
    gr_re_pattern = re.compile('GRATIS_(\d+)')
    gr_ids = [int(re.findall(gr_re_pattern, p)[0]) for p in gr_filenames]

    ids = list(set(dz_ids + gr_ids))
    ddict = collections.defaultdict(dict)

    for i in ids:
        ddict[i]['dz'] = None
        ddict[i]['gr'] = None

    for i, f in zip(dz_ids, dz_filenames):
        ddict[i]['dz'] = os.path.join(dir_path, f)

    for i, f in zip(gr_ids, gr_filenames):
        ddict[i]['gr'] = os.path.join(dir_path, f)

    paths = [(k, v['dz'], v['gr']) for k, v in ddict.items()]
    print(paths)

    r = list(tqdm.tqdm(pool.imap(process_paths, paths), total=len(paths)))
