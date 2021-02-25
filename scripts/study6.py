import numpy as np
import multiprocessing as mp
import os
import pandas as pd
import re
import tqdm
import traceback


# condition.xlsx
co = pd.read_excel('data/condition6a.xlsx', 'study6')

st = pd.read_excel('data/condition6.xlsx', 'list of stimuli')

# missing.xlsx
mi = pd.read_excel('data/missing.xlsx', 'Study6')

baseline_marker_id = '-1'

emodive_re_pattern = re.compile('EMODIVE_(\d+)')

dir_path = 'data/Study 6'
dzdt_filenames = [p for p in os.listdir(dir_path) if '_DZDT.txt' in p]


def process_path(emodive_path):
    try:
        subject_id = int(re.findall(emodive_re_pattern, emodive_path)[0].lstrip('0'))

        emodiveadv_path = f'data/Study 6/emodiveadv{subject_id}.csv'
        emodivebas_path = f'data/Study 6/emodivebas{subject_id}.csv'

        ADV = os.path.exists(emodiveadv_path)
        BAS = os.path.exists(emodivebas_path)

        # condition.xlsx
        subject_row = co.loc[co['Subject_ID'] == subject_id].iloc[0, :]  # first row for study5, second for study5a
        subject_sex = int(subject_row['płeć'])

        try:
            subject_age = int(subject_row['wiek'])
        except Exception as e:
            subject_age = 0

        try:
            film1_marker_id = str(int(subject_row['film1']))
            film2_marker_id = str(int(subject_row['film2']))
            film3_marker_id = str(int(subject_row['film3']))
            film4_marker_id = str(int(subject_row['film4']))
            film5_marker_id = str(int(subject_row['film5']))
            film6_marker_id = str(int(subject_row['film6']))
        except Exception as e:
            print(f'{emodive_path} - some film marker does not exist for this subject, processing skipped.')
            return 0

        film1_marker_name = st[st.iloc[:, 2] == int(film1_marker_id)].iloc[:, 4].item()
        film2_marker_name = st[st.iloc[:, 2] == int(film2_marker_id)].iloc[:, 4].item()
        film3_marker_name = st[st.iloc[:, 2] == int(film3_marker_id)].iloc[:, 4].item()
        film4_marker_name = st[st.iloc[:, 2] == int(film4_marker_id)].iloc[:, 4].item()
        film5_marker_name = st[st.iloc[:, 2] == int(film5_marker_id)].iloc[:, 4].item()
        film6_marker_name = st[st.iloc[:, 2] == int(film6_marker_id)].iloc[:, 4].item()

        # emodive
        column_names = ['timestamp', 'meter', 'respiration', 'SC', 'marker_em']
        dtype_dict = {k: 'float' for k in column_names}
        dtype_dict['marker_em'] = 'string'

        em = pd.read_csv(emodive_path, sep='\t', header=None, skiprows=9, encoding='cp1250', decimal=',', names=column_names, dtype=dtype_dict)
        em = em.drop(columns=['respiration'])
        em['marker_em'] = em['marker_em'].apply(lambda x: x.strip() if type(x) == str else x)
        em['marker_em'] = em['marker_em'].replace(float('nan'), '')

        # load dz, dzdt, ecg, z0
        dzdt_re_pattern = re.compile(f'^0*{subject_id}_.*_DZDT.txt')
        try:
            dzdt_filename = [p for p in dzdt_filenames if re.fullmatch(dzdt_re_pattern, p)][0]
            DZDT = True
        except:
            DZDT = False

        if DZDT:
            dzdt_path = os.path.join(dir_path, dzdt_filename)
            dz_path = dzdt_path.replace('DZDT', 'DZ')
            ecg_path = dzdt_path.replace('DZDT', 'ECG')
            z0_path = dzdt_path.replace('DZDT', 'Z0')

            dz = pd.read_csv(dz_path, sep=' ', header=None, skiprows=3, names=['sample_id', 'dz', 'dz_peak', 'marker_dz', 'val4'])
            dzdt = pd.read_csv(dzdt_path, sep=' ', header=None, skiprows=3, names=['sample_id', 'dzdt', 'val2', 'val3', 'val4'])
            ecg = pd.read_csv(ecg_path, sep=' ', header=None, skiprows=3, names=['sample_id', 'ecg', 'val2', 'val3', 'val4'])
            z0 = pd.read_csv(z0_path, sep=' ', header=None, skiprows=3, names=['sample_id', 'z0', 'val2', 'val3', 'val4'])

            dz = dz.drop(columns=['val4'])

            # merge dataframes
            df = pd.merge(dz, dzdt[['sample_id', 'dzdt']], on='sample_id', how='inner')
            df = pd.merge(df, ecg[['sample_id', 'ecg']], on='sample_id', how='inner')
            df = pd.merge(df, z0[['sample_id', 'z0']], on='sample_id', how='left')
            del dz, dzdt, ecg, z0

            # interpolate z0
            df = df.fillna(method='ffill')
            df = df.fillna(method='bfill')

            # sync df + em
            DF_MARKER_101 = df['marker_dz'].isin([101]).any()
            if DF_MARKER_101:
                df_sync_idx = df.index[df['marker_dz'] == 101][0]
                em_sync_idx = em.index[em['marker_em'] == '#* baseline'][0]

                rows_diff = df_sync_idx - em_sync_idx

                if rows_diff > 0:  # if em recording started LATER
                    df = df.iloc[rows_diff:]
                    df = df.reset_index(drop=True)
                else:
                    em = em.iloc[-rows_diff:]
                    em = em.reset_index(drop=True)

            else:
                print(f'{dz_path} - No sync marker "101", fill dz, dzdt, ecg, z0 with nan.')
                df = pd.DataFrame(index=em.index, columns=['sample_id', 'dz', 'dz_peak', 'marker_dz', 'dzdt', 'ecg', 'z0'])
                df = df.fillna(value=float('nan'))

        else:
            print(f'{emodive_path} - DZDT for this subject does not exist, fill dz, dzdt, ecg, z0 with nan.')
            df = pd.DataFrame(index=em.index, columns=['sample_id', 'dz', 'dz_peak', 'marker_dz', 'dzdt', 'ecg', 'z0'])
            df = df.fillna(value=float('nan'))

        df = df.merge(em, how='inner', left_index=True, right_index=True)

        # adv
        if ADV:
            column_names = ['Time', 'SV', 'CO', 'SVI', 'CI', 'dp-dt', 'SPTI', 'RPP', 'DPTI', 'DPTI-SPTI', 'LVET', 'ZAo', 'Cwk', 'Rp', 'TPR', 'BSA', 'TPRI', 'maxAortaArea', 'marker_adv', 'Region', 'dummy']
            dtype_dict = {k: 'float' for k in column_names}
            dtype_dict['marker_adv'] = 'string'

            adv = pd.read_csv(emodiveadv_path, sep=';', header=None, skiprows=9, encoding='cp1250', names=column_names, dtype=dtype_dict)
            adv = adv.drop(columns=['SV', 'SVI', 'CI', 'dp-dt', 'SPTI', 'RPP', 'DPTI', 'DPTI-SPTI', 'LVET', 'ZAo', 'Cwk', 'Rp', 'BSA', 'TPRI', 'maxAortaArea', 'Region', 'dummy'])
            adv['marker_adv'] = adv['marker_adv'].apply(lambda x: x.strip() if type(x) == str else x)
            adv['marker_adv'] = adv['marker_adv'].replace(float('nan'), '')

            adv['Timedelta'] = adv.apply(lambda x: pd.to_timedelta(x['Time'], unit='s'), axis=1)
            adv = adv.set_index('Timedelta')

            adv = adv.resample('ms').ffill()

            adv['Time'] = adv.index.total_seconds()
            adv = adv.reset_index(drop=True)

        # bas
        if BAS:
            column_names = ['Time', 'fiSYS', 'fiMAP', 'fiDIA', 'reSYS', 'reMAP', 'reDIA', 'PhysioCalActive', 'noBeatDetected', 'IBI', 'HR AP', 'marker_bas', 'Region', 'dummy']
            dtype_dict = {k: 'float' for k in column_names}
            dtype_dict['marker_bas'] = 'string'

            bas = pd.read_csv(emodivebas_path, sep=';', header=None, skiprows=8, encoding='cp1250', names=column_names, dtype=dtype_dict)
            bas = bas.drop(columns=['fiSYS', 'fiMAP', 'fiDIA', 'reMAP', 'PhysioCalActive', 'noBeatDetected', 'IBI', 'HR AP', 'Region', 'dummy'])
            bas['marker_bas'] = bas['marker_bas'].apply(lambda x: x.strip() if type(x) == str else x)
            bas['marker_bas'] = bas['marker_bas'].replace(float('nan'), '')

            bas_marker = bas[bas['marker_bas'] == 'markeremo'].iloc[0]

            bas['Timedelta'] = bas.apply(lambda x: pd.to_timedelta(x['Time'], unit='s'), axis=1)
            bas = bas.set_index('Timedelta')
            bas = bas.drop(columns=['Time'])

            bas = bas.dropna(axis=0, subset=['reSYS', 'reDIA'], how='any')

            bas = bas.resample('ms').ffill()
            bas['Time'] = bas.index.total_seconds()
            bas = bas.reset_index(drop=True)

            bas.loc[np.isclose(bas['Time'], bas_marker['Time'], rtol=0, atol=1e-05), 'marker_bas'] = bas_marker['marker_bas']

        # sync adv + bas
        if ADV and BAS:
            adv_sync_idx = adv.index[adv['marker_adv'] == 'markeremo'][0]
            bas_sync_idx = bas.index[bas['marker_bas'] == 'markeremo'][0]

            rows_diff = adv_sync_idx - bas_sync_idx

            if rows_diff > 0:  # if bas recording started LATER
                adv = adv.iloc[rows_diff:]
                adv = adv.reset_index(drop=True)
            else:
                bas = bas.iloc[-rows_diff:]
                bas = bas.reset_index(drop=True)

        elif ADV and not BAS:
            print(f'{emodivebas_path} does not exist, fill SBP, DBP with nan.')
            bas = pd.DataFrame(index=adv.index, columns=['Time', 'reSYS', 'reDIA'])
            bas = bas.fillna(value=float('nan'))

        elif not ADV and BAS:
            print(f'{emodiveadv_path} does not exist, fill CO, TPR with nan.')
            adv = pd.DataFrame(index=bas.index, columns=['Time', 'CO', 'TPR'])
            adv = adv.fillna(value=float('nan'))

        if ADV or BAS:
            ab = adv.merge(bas, how='inner', left_index=True, right_index=True)
            ab = ab.drop(columns=['Time_y'], errors='ignore')

            # sync df + ab
            if ADV:
                ab_sync_idx = ab.index[ab['marker_adv'] == 'markeremo'][0]
            else:  # if BAS
                ab_sync_idx = ab.index[ab['marker_bas'] == 'markeremo'][0]

            DF_MARKER_EM_M = df['marker_em'].isin(['#* m']).any()
            if DF_MARKER_EM_M:
                df_sync_idx = df.index[df['marker_em'] == '#* m'][0]

                rows_diff = df_sync_idx - ab_sync_idx

                if rows_diff > 0:  # if ab recording started LATER
                    df = df.iloc[rows_diff:]
                    df = df.reset_index(drop=True)
                else:
                    ab = ab.iloc[-rows_diff:]
                    ab = ab.reset_index(drop=True)

            else:
                print(f'{emodive_path} - no "#* m" marker, cannot sync ADV and/or BAS, fill SBP, DBP, CO, TPR with nan.')
                ab = pd.DataFrame(index=df.index, columns=['Time', 'reSYS', 'reDIA', 'CO', 'TPR'])
                ab = ab.fillna(value=float('nan'))

        else:
            print(f'{emodive_path} - no ADV and BAS files, fill SBP, DBP, CO, TPR with nan.')
            ab = pd.DataFrame(index=df.index, columns=['Time', 'reSYS', 'reDIA', 'CO', 'TPR'])
            ab = ab.fillna(value=float('nan'))

        df = df.merge(ab, how='inner', left_index=True, right_index=True)

        # tidy up
        df = df[['timestamp', 'meter', 'ecg', 'dzdt', 'dz', 'z0', 'SC', 'reSYS', 'reDIA', 'CO', 'TPR', 'marker_em']].copy()
        df['marker'] = float('nan')
        df = df.rename(columns={'ecg': 'ECG', 'SC': 'EDA', 'reSYS': 'SBP', 'reDIA': 'DBP'})

        # periods

        # baseline, 101
        df_idx_base = df.index[df['marker_em'] == '#* baseline'][0]
        df.loc[df_idx_base:df_idx_base + 300000, 'marker'] = baseline_marker_id

        # movie1, 12
        df_idx_film1 = df.index[df['marker_em'] == '#* movies'][0]
        df.loc[df_idx_film1:df_idx_film1 + 120000, 'marker'] = film1_marker_id

        # movie2, 13
        df_idx_film2 = df_idx_film1 + 120000
        df.loc[df_idx_film2:df_idx_film2 + 120000, 'marker'] = film2_marker_id

        # movie3, 14
        df_idx_film3 = df_idx_film2 + 120000
        df.loc[df_idx_film3:df_idx_film3 + 120000, 'marker'] = film3_marker_id

        # movie4, 15
        df_idx_film4 = df_idx_film3 + 120000
        df.loc[df_idx_film4:df_idx_film4 + 120000, 'marker'] = film4_marker_id

        # movie5, 16
        df_idx_film5 = df_idx_film4 + 120000
        df.loc[df_idx_film5:df_idx_film5 + 120000, 'marker'] = film5_marker_id

        # movie6, 17
        df_idx_film6 = df_idx_film5 + 120000
        df.loc[df_idx_film6:df_idx_film6 + 120000, 'marker'] = film6_marker_id

        # missing.xlsx
        missing_values = mi.loc[mi['id'] == subject_id].iloc[:, 1:4].values[0]
        for missing_value in missing_values:
            if missing_value == 'ecg, dz, dz/dt, zo':
                for c in ['ECG', 'dz', 'dzdt', 'z0']:
                    df[c] = float('nan')

            elif missing_value == 'SBP, DBP, CO, TPR':
                for c in ['SBP', 'DBP', 'CO', 'TPR']:
                    df[c] = float('nan')

            elif missing_value == 'scl':
                df['EDA'] = float('nan')

            elif missing_value == 'ecg':
                df['ECG'] = float('nan')

        for c in ['timestamp']:
            df.loc[:, c] = df[c].map(lambda x: '%.10g' % x)

        for c in ['ECG', 'dz', 'dzdt', 'z0', 'TPR']:
            if df[c].isnull().all():
                continue

            df.loc[:, c] = df[c].map(lambda x: '%.3f' % x)

        df = df.drop(columns=['marker_dz', 'marker_em'], errors='ignore')
        df = df.rename(columns={'meter': 'affect'})

        header = f"""#Study_name,Study 6
#Subject_ID,{subject_id}
#Subject_Age,{subject_age}
#Subject_Sex,{subject_sex}
#Channel_Name,timestamp,affect,ECG,dzdt,dz,z0,EDA,SBP,DBP,CO,TPR,marker
#Data_Category,timestamp,data,data,data,data,data,data,data,data,data,data,marker
#Data_Unit,second,custom,millivolts,ohm/s,ohm,ohm,microsiemens,mmHg,mmHg,l/min,mmHg*min/l,string
#Data_Sample_rate,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,Beat-to-beat,Beat-to-beat,Beat-to-beat,Beat-to-beat,1000Hz
#Data_Device,LabChart 8.19 (ADInsturments, New Zealand),Response Meter (ADInsturments, New Zealand),ECG (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),ICG  (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),ICG  (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),ICG  (Vrije Universiteit Ambulatory Monitoring System, VU-AMS, the Netherlands),GSR Amp (ADInstruments, New Zealand),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Labchart 8.19 (AdInsturments, New Zealand)
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

        df_film6 = df[df_idx_film6:df_idx_film6 + 120000]
        with open(f'{subject_id}_{film6_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_film6.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        with open(f'{subject_id}_All.csv', 'w', newline='') as f:
            f.write(header)
            df.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

    except Exception as e:
        print(emodive_path, '\n', repr(e), '\n', traceback.format_exc())


if __name__ == '__main__':
    pool = mp.Pool(8)
    emodive_paths = [os.path.join(dir_path, p) for p in os.listdir(dir_path) if 'EMODIVE_' in p]
    r = list(tqdm.tqdm(pool.imap(process_path, emodive_paths), total=len(emodive_paths)))
