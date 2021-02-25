import multiprocessing as mp
import os
import pandas as pd
import re
import tqdm
import traceback


# condition.xlsx
co = pd.read_excel('data/condition5.xlsx', 'study5')

st = pd.read_excel('data/condition6.xlsx', 'list of stimuli')

# missing.xlsx
mi = pd.read_excel('data/missing.xlsx', 'Study5')

baseline_marker_id = '-1'

moca_re_pattern = re.compile('MOCA_os(\d+)')


def process_path(moca_path):
    try:
        subject_id = int(re.findall(moca_re_pattern, moca_path)[0].lstrip('0'))
        mocafino_path = f'data/Study 5a/MocaFino{subject_id}.txt'
        FI = os.path.exists(mocafino_path)

        # condition.xlsx
        subject_row = co.loc[co['Subject'] == subject_id].iloc[1, :]  # first row for study5, second for study5a
        subject_sex = int(subject_row['płecM0K1'])
        subject_age = int(subject_row['wiek'])
        output_subject_id = int(subject_row['id_manual'])
        try:
            film1_marker_id = str(int(subject_row['Film1']))
            film2_marker_id = str(int(subject_row['Film2']))
            film3_marker_id = str(int(subject_row['Film3']))
        except Exception as e:
            print(f'{moca_path} - some film marker does not exist for this subject (in condition.xlsx), processing skipped.')
            return 0

        film1_marker_name = st[st.iloc[:, 2] == int(film1_marker_id)].iloc[:, 4].item()
        film2_marker_name = st[st.iloc[:, 2] == int(film2_marker_id)].iloc[:, 4].item()
        film3_marker_name = st[st.iloc[:, 2] == int(film3_marker_id)].iloc[:, 4].item()

        # MOCA
        column_names = ['timestamp', 'meter', 'ECG', 'EDA', 'marker_ed']
        dtype_dict = {k: 'float' for k in column_names}
        dtype_dict['marker_ed'] = 'string'

        ed = pd.read_csv(moca_path, sep='\t', header=None, skiprows=9, decimal=',',names=column_names, dtype=dtype_dict)
        ed['marker_ed'] = ed['marker_ed'].apply(lambda x: x.strip() if type(x) == str else x)

        for marker_ed in ['#* f', '#* a1', '#* a2', '#* a3']:
            if not ed['marker_ed'].isin([marker_ed]).any():
                print(f'{moca_path} does not have {marker_ed} marker, cannot sync FI (or cannot generate all periods).')
                FI = False

        # MocaFino
        if FI:
            column_names = ['Time', 'SBP', 'DBP', 'MBP', 'HR', 'SV', 'LVET', 'PI', 'MS', 'CO', 'TPR', 'TPRCGS', 'marker_fi', 'empty']
            dtype_dict = {k: 'float' for k in column_names}
            dtype_dict['marker_fi'] = 'string'

            fi = pd.read_csv(mocafino_path, sep=';', decimal=',', header=None, skiprows=9, names=column_names, dtype=dtype_dict, parse_dates=['Time'])
            fi = fi[['Time', 'SBP', 'DBP', 'CO', 'TPR', 'marker_fi']]

            FI_MARKER_M = fi['marker_fi'].isin(['m']).any()
            if FI_MARKER_M:
                fi = fi.set_index('Time')
                fi = fi.resample('ms').ffill()
                fi['Time_fi'] = fi.index
                fi['Time_fi'] = fi['Time_fi'].dt.time
                fi = fi.reset_index(drop=True)

                fi['marker_fi'] = fi['marker_fi'].replace(float('nan'), '')

                # synchronization
                ed_sync_idx = ed.index[ed['marker_ed'] == '#* f'][0]
                fi_sync_idx = fi.index[fi['marker_fi'] == 'm'][0]

                rows_diff = ed_sync_idx - fi_sync_idx

                if rows_diff > 0:  # if MocaFino recording started LATER
                    ed = ed.iloc[rows_diff:]
                    ed = ed.reset_index(drop=True)
                else:
                    fi = fi.iloc[-rows_diff:]
                    fi = fi.reset_index(drop=True)

            else:
                print(f'{mocafino_path} does not have "m" marker, fill SBP, DBP, CO, TRP with nan.')
                fi = pd.DataFrame(index=ed.index, columns=['Time_fi', 'SBP', 'DBP', 'CO', 'TPR', 'marker_fi'])
                fi = fi.fillna(value=float('nan'))

        else:
            print(f'{mocafino_path} does not exist, fill SBP, DBP, CO, TRP with nan.')
            fi = pd.DataFrame(index=ed.index, columns=['Time_fi', 'SBP', 'DBP', 'CO', 'TPR', 'marker_fi'])
            fi = fi.fillna(value=float('nan'))

        df = ed.merge(fi, how='inner', left_index=True, right_index=True)
        del ed, fi

        df = df.drop(columns=['Time_fi', 'marker_fi'])
        df['marker'] = float('nan')

        # periods
        # baseline ('a1' - 300 s)
        df_idx_base = df.index[df['marker_ed'] == '#* a1'][0] - 300000
        df.loc[df_idx_base:df_idx_base + 300000, 'marker'] = baseline_marker_id

        # Film1 ('a1' + 120 s)
        df_idx_film1 = df.index[df['marker_ed'] == '#* a1'][0]
        df.loc[df_idx_film1:df_idx_film1 + 120000, 'marker'] = film1_marker_id

        # Film2 ('a2' + 120 s)
        df_idx_film2 = df.index[df['marker_ed'] == '#* a2'][0]
        df.loc[df_idx_film2:df_idx_film2 + 120000, 'marker'] = film2_marker_id

        # Film3 ('a3' + 120 s)
        df_idx_film3 = df.index[df['marker_ed'] == '#* a3'][0]
        df.loc[df_idx_film3:df_idx_film3 + 120000, 'marker'] = film3_marker_id

        df = df.drop(columns=['marker_ed'])
        df = df.rename(columns={'meter': 'affect'})

        # missing.xlsx
        missing_value = mi.loc[mi['id'] == subject_id].iloc[:, 1].item()

        if missing_value == 'SBP, DBP, CO, TPR':
            for c in ['SBP', 'DBP', 'CO', 'TPR']:
                df[c] = float('nan')
        elif missing_value == 'ecg':
            df['ECG'] = float('nan')

        for c in ['timestamp']:
            df.loc[:, c] = df[c].map(lambda x: '%.10g' % x)

        for c in ['ECG', 'TPR']:
            if df[c].isnull().all():
                continue

            df.loc[:, c] = df[c].map(lambda x: '%.3f' % x)

        # to .csv
        header = f"""#Study_name,Study 5
#Subject_ID,{output_subject_id}
#Subject_Age,{subject_age}
#Subject_Sex,{subject_sex}
#Channel_Name,timestamp,affect,ECG,EDA,SBP,DBP,CO,TPR,marker
#Data_Category,timestamp,data,data,data,data,data,data,data,marker
#Data_Unit,second,custom,millivolts,microsiemens,mmHg,mmHg,l/min,mmHg*min/l,string
#Data_Sample_rate,1000Hz,1000Hz,1000Hz,1000Hz,Beat-to-beat,Beat-to-beat,Beat-to-beat,Beat-to-beat,1000Hz
#Data_Device,LabChart 8.19 (ADInsturments, New Zealand),Response Meter (ADInsturments, New Zealand),ECG (ADInsturments, New Zealand),GSR Amp (ADInstruments, New Zealand),Finometer MIDI (Finapres Medical Systems, Netherlands),Finometer MIDI (Finapres Medical Systems, Netherlands),Finometer MIDI (Finapres Medical Systems, Netherlands),Finometer MIDI (Finapres Medical Systems, Netherlands),Labchart 8.19 (AdInsturments, New Zealand)
"""

        df_base = df[df_idx_base:df_idx_base + 300000]
        with open(f'{output_subject_id}_Baseline.csv', 'w', newline='') as f:
            f.write(header)
            df_base.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_film1 = df[df_idx_film1:df_idx_film1 + 120000]
        with open(f'{output_subject_id}_{film1_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film1.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_film2 = df[df_idx_film2:df_idx_film2 + 120000]
        with open(f'{output_subject_id}_{film2_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film2.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_film3 = df[df_idx_film3:df_idx_film3 + 120000]
        with open(f'{output_subject_id}_{film3_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film3.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        with open(f'{output_subject_id}_All.csv', 'w', newline='') as f:
            f.write(header)
            df.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

    except Exception as e:
        print(moca_path, '\n', repr(e), '\n', traceback.format_exc())


if __name__ == '__main__':
    pool = mp.Pool(8)
    dir_path = 'data/Study 5a'
    moca_paths = [os.path.join(dir_path, p) for p in os.listdir(dir_path) if 'MOCA_' in p]
    r = list(tqdm.tqdm(pool.imap(process_path, moca_paths), total=len(moca_paths)))
