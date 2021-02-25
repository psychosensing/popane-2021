import multiprocessing as mp
import os
import pandas as pd
import tqdm
import traceback

# missing.xlsx
mi = pd.read_excel('data/missing.xlsx', 'Study4')

# condition.xlsx
co = pd.read_excel('data/condition5.xlsx', 'study4')

st = pd.read_excel('data/condition6.xlsx', 'list of stimuli')

baseline_marker_id = '-1'


def process_path(darkfear_filepath):
    try:
        subject_id = int(darkfear_filepath.split(' ')[-1].rstrip('.txt').lstrip('0'))

        # condition.xlsx
        try:
            subject_age = int(co.loc[co['id'] == subject_id, 'age'].item())
        except Exception:
            subject_age = -1

        try:
            subject_sex = int(co.loc[co['id'] == subject_id, 'sex'].item())
        except Exception:
            subject_sex = -1

        film_marker_id = int(co.loc[co['id'] == subject_id, 'condition'].item())
        film_marker_name = st[st.iloc[:, 2] == film_marker_id].iloc[:, 4].item()

        # darkfear
        column_names = ['timestamp', 'EKG', 'SC', 'marker_da']
        dtype_dict = {k: 'float' for k in column_names}
        dtype_dict['marker_da'] = 'string'

        with open(darkfear_filepath) as f:
            for i, line in enumerate(f):
                if 'BottomValue' in line:
                    rows_to_skip = i + 1  # for files with multiple headers inside

        da = pd.read_csv(darkfear_filepath, sep='\t', header=None, skiprows=rows_to_skip, encoding='cp1250', decimal=',', names=column_names, dtype=dtype_dict)
        da['marker_da'] = da['marker_da'].apply(lambda x: x.strip() if type(x) == str else x)

        # .txt
        txt_filepath = os.path.join(os.path.dirname(darkfear_filepath), f'{subject_id}.txt')
        TX = os.path.exists(txt_filepath)

        if TX:
            column_names = ['timestamp', 'SBP', 'DBP', 'MBP', 'HR', 'SV', 'LVET', 'PI', 'MS', 'CO', 'TPR', 'TPRCGS', 'marker_tx', 'dummy']
            dtype_dict = {k: 'float' for k in column_names}
            dtype_dict['marker_tx'] = 'string'

            tx = pd.read_csv(txt_filepath, sep=';', header=None, skiprows=9, encoding='cp1250', decimal=',', names=column_names, dtype=dtype_dict, parse_dates=['timestamp'])
            tx = tx.drop(columns=['dummy', 'MBP', 'HR', 'SV', 'LVET', 'PI', 'MS', 'TPRCGS'])

            # resample.txt
            tx = tx.set_index('timestamp')
            tx = tx.resample('ms').ffill()
            tx = tx.reset_index()
            tx['timestamp'] = tx['timestamp'].dt.time

            # find synchronization point
            da_sync_idx = da.index[da['marker_da'] == '#* f'][0]
            tx_sync_idx = tx.index[tx['marker_tx'].notna()][0]
            assert tx.at[tx_sync_idx, 'marker_tx'].startswith('m')

            rows_diff = tx_sync_idx - da_sync_idx
            # print(rows_diff)
            if rows_diff > 0:  # if darkfear recording started LATER
                tx = tx.iloc[rows_diff:]
                tx = tx.reset_index(drop=True)
            else:
                da = da.iloc[-rows_diff:]
                da = da.reset_index(drop=True)

        else:
            print(f'{txt_filepath} does not exist; fill SBP, DBP, CO, TRP with nan.')

            tx = pd.DataFrame(index=da.index, columns=['timestamp', 'SBP', 'DBP', 'CO', 'TPR'])
            tx = tx.fillna(value=float('nan'))

        # merge
        df = da.merge(tx, how='inner', left_index=True, right_index=True)
        del tx

        df = df[['timestamp_x', 'EKG', 'SC', 'SBP', 'DBP', 'CO', 'TPR', 'marker_da']]
        df['marker'] = ''

        # periods
        df_idx_base = df.index[df['marker_da'] == '#* BaselineStarts'][0]
        df.loc[df_idx_base:df_idx_base + 300000, 'marker'] = baseline_marker_id

        df_idx_film = df.index[df['marker_da'] == '#* FilmStarts'][0]
        df.loc[df_idx_film:df_idx_film + 300000, 'marker'] = film_marker_id

        # missing.xlsx
        missing_value = mi.loc[mi['id'] == subject_id].iloc[:, 1].item()

        if missing_value == 'SBP, DBP, CO, TPR':
            print(f'Subject {subject_id} - "SBP, DBP, CO, TPR" in missing.xlsx')
            for c in ['SBP', 'DBP', 'CO', 'TPR']:
                df[c] = float('nan')

        # to .csv
        df = df.drop(columns=['marker_da'])
        df = df.rename(columns={'timestamp_x': 'timestamp', 'EKG': 'ECG', 'SC': 'EDA'})

        for c in ['timestamp']:
            df.loc[:, c] = df[c].map(lambda x: '%.10g' % x)

        for c in ['ECG', 'TPR']:
            if df[c].isnull().all():
                continue

            df.loc[:, c] = df[c].map(lambda x: '%.3f' % x)

        header = f"""#Study_name,Study 4
#Subject_ID,{subject_id}
#Subject_Age,{subject_age}
#Subject_Sex,{subject_sex}
#Channel_Name,timestamp,ECG,EDA,SBP,DBP,CO,TPR,marker
#Data_Category,timestamp,data,data,data,data,data,data,marker
#Data_Unit,second,millivolts,microsiemens,mmHg,mmHg,l/min,mmHg*min/l,string
#Data_Sample_rate,1000Hz,1000Hz,1000Hz,Beat-to-beat,Beat-to-beat,Beat-to-beat,Beat-to-beat,1000Hz
#Data_Device,LabChart 8.19 (ADInsturments, New Zealand),ECG (ADInsturments, New Zealand),GSR Amp (ADInstruments, New Zealand),Finometer MIDI (Finapres Medical Systems, Netherlands),Finometer MIDI (Finapres Medical Systems, Netherlands),Finometer MIDI (Finapres Medical Systems, Netherlands),Finometer MIDI (Finapres Medical Systems, Netherlands),Labchart 8.19 (AdInsturments, New Zealand)
"""

        df_base = df[df_idx_base:df_idx_base + 300000]

        with open(f'{subject_id}_Baseline.csv', 'w', newline='') as f:
            f.write(header)
            df_base.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_film = df[df_idx_film:df_idx_film + 221000]

        with open(f'{subject_id}_{film_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        with open(f'{subject_id}_All.csv', 'w', newline='') as f:
            f.write(header)
            df.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

    except Exception as e:
        print(darkfear_filepath, txt_filepath, '\n', repr(e), '\n', traceback.format_exc())


if __name__ == '__main__':
    pool = mp.Pool()
    dir_path = 'data/Study 4'
    darkfear_paths = [os.path.join(dir_path, p) for p in os.listdir(dir_path) if 'DARKFEAR' in p]
    r = list(tqdm.tqdm(pool.imap(process_path, darkfear_paths), total=len(darkfear_paths)))
