import numpy as np
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
        moca_adv_path = f'data/Study 5/MocaNovaAdv{subject_id}.csv'
        moca_bas_path = f'data/Study 5/MocaNovaBas{subject_id}.csv'

        ADV = os.path.exists(moca_adv_path)
        BAS = os.path.exists(moca_bas_path)

        if not ADV:
            print(f'{moca_adv_path} does not exist.')

        if not BAS:
            print(f'{moca_bas_path} does not exist.')

        # condition.xlsx
        subject_row = co.loc[co['Subject'] == subject_id].iloc[0, :]  # first row for study5, second for study5a
        subject_sex = int(subject_row['płecM0K1'])
        subject_age = int(subject_row['wiek'])
        output_subject_id = int(subject_row['id_manual'])
        try:
            film1_marker_id = str(int(subject_row['Film1']))
            film2_marker_id = str(int(subject_row['Film2']))
            film3_marker_id = str(int(subject_row['Film3']))
        except Exception as e:
            print(f'{moca_path} - some film marker does not exist for this subject, processing skipped.')
            return 0

        film1_marker_name = st[st.iloc[:, 2] == int(film1_marker_id)].iloc[:, 4].item()
        film2_marker_name = st[st.iloc[:, 2] == int(film2_marker_id)].iloc[:, 4].item()
        film3_marker_name = st[st.iloc[:, 2] == int(film3_marker_id)].iloc[:, 4].item()

        # Moca
        column_names = ['timestamp', 'meter1', 'ecg1', 'sc1', 'marker_ed']
        dtype_dict = {k: 'float' for k in column_names}
        dtype_dict['marker_ed'] = 'string'

        ed = pd.read_csv(moca_path, sep='\t', header=None, skiprows=9, decimal=',', names=column_names, dtype=dtype_dict)
        ed['marker_ed'] = ed['marker_ed'].apply(lambda x: x.strip() if type(x) == str else x)

        for marker_ed in ['#* m', '#* 1', '#* 4', '#* 12', '#* 20']:
            if not ed['marker_ed'].isin([marker_ed]).any():
                print(f'{moca_path} does not have {marker_ed} marker, cannot sync ADV and BAS (or cannot generate all periods).')
                ADV = False
                BAS = False

        # MocaNovaAdv
        if ADV:
            column_names = ['Time','SV','CO','SVI','CI','dp-dt','SPTI','RPP','DPTI','DPTI-SPTI','LVET','ZAo','Cwk','Rp','TPR','BSA','TPRI','maxAortaArea','marker_adv','Region','empty']
            dtype_dict = {k: 'float' for k in column_names}
            dtype_dict['marker_adv'] = 'string'

            adv = pd.read_csv(moca_adv_path, sep=';', header=None, skiprows=8, names=column_names, dtype=dtype_dict)
            adv = adv[['Time', 'CO', 'TPR', 'marker_adv']]

            # MocaNovaAdv - resample
            adv['Timedelta'] = adv.apply(lambda x: pd.to_timedelta(x['Time'], unit='s'), axis=1)
            adv = adv.set_index('Timedelta')
            adv = adv.drop(columns=['Time'])

            adv = adv.resample('ms').ffill()

            adv['Time'] = adv.index.total_seconds()
            adv = adv.reset_index(drop=True)

            adv['marker_adv'] = adv['marker_adv'].replace(float('nan'), '')

        # MocaNovaBas
        if BAS:
            column_names = ['Time','fiSYS','fiMAP','fiDIA','reSYS','reMAP','reDIA','PhysioCalActive','noBeatDetected','IBI','HR AP','marker_bas','Region','empty']
            dtype_dict = {k: 'float' for k in column_names}
            dtype_dict['marker_bas'] = 'string'

            bas = pd.read_csv(moca_bas_path, sep=';', header=None, skiprows=8, names=column_names)  #, dtype=dtype_dict)
            bas = bas[['Time', 'reSYS', 'reDIA', 'marker_bas']]

            try:
                bas_marker = bas[bas['marker_bas'].notna()].iloc[0]
            except Exception as e:
                print(f'{moca_bas_path} does not have markers, processing skipped.')
                return 0

            # MocaNovaBas - resample
            bas['Timedelta'] = bas.apply(lambda x: pd.to_timedelta(x['Time'], unit='s'), axis=1)
            bas = bas.set_index('Timedelta')
            bas = bas.drop(columns=['Time'])

            bas = bas.dropna(axis=0, subset=['reSYS', 'reDIA'], how='any')

            bas = bas.resample('ms').ffill()

            bas['Time'] = bas.index.total_seconds()
            bas = bas.reset_index(drop=True)

            bas.loc[np.isclose(bas['Time'], bas_marker['Time'], rtol=0, atol=1e-05), 'marker_bas'] = bas_marker['marker_bas']

            bas['marker_bas'] = bas['marker_bas'].replace(float('nan'), '')

        # synchronize adv with bas to nova (no = nova)
        if ADV or BAS:
            if ADV and BAS:
                # find synchronization point
                adv_sync_idx = adv.index[adv['marker_adv'] == 'markermoc'][0]
                bas_sync_idx = bas.index[bas['marker_bas'] == 'markermoc'][0]

                rows_diff = adv_sync_idx - bas_sync_idx

                if rows_diff > 0:  # if bas recording started LATER
                    adv = adv.iloc[rows_diff:]
                    adv = adv.reset_index(drop=True)
                else:
                    bas = bas.iloc[-rows_diff:]
                    bas = bas.reset_index(drop=True)

                no = adv.merge(bas, how='inner', left_index=True, right_index=True)
                no_sync_idx = no.index[no['marker_adv'] == 'markermoc'][0]

            elif ADV and not BAS:
                no = adv.copy()
                no['reSYS'] = float('nan')
                no['reDIA'] = float('nan')
                no_sync_idx = no.index[no['marker_adv'] == 'markermoc'][0]

            elif not ADV and BAS:
                no = bas.copy()
                no['CO'] = float('nan')
                no['TPR'] = float('nan')
                no_sync_idx = no.index[no['marker_bas'] == 'markermoc'][0]

            del adv, bas

            # find synchronization point
            ed_sync_idx = ed.index[ed['marker_ed'] == '#* m'][0]

            rows_diff = ed_sync_idx - no_sync_idx

            if rows_diff > 0:  # if adv/bas recording started LATER
                ed = ed.iloc[rows_diff:]
                ed = ed.reset_index(drop=True)
            else:
                no = no.iloc[-rows_diff:]
                no = no.reset_index(drop=True)

        else:
            no = pd.DataFrame(index=ed.index, columns=['reSYS', 'reDIA', 'CO', 'TPR'])
            no = no.fillna(value=float('nan'))

        # merge
        df = ed.merge(no, how='inner', left_index=True, right_index=True)
        del ed, no

        df = df.drop(columns=['Time', 'Time_x', 'Time_y', 'marker_adv', 'marker_bas'], errors='ignore')
        df['marker'] = float('nan')

        # periods
        df_idx_base = df.index[df['marker_ed'] == '#* 1'][0]
        df.loc[df_idx_base:df_idx_base+300000, 'marker'] = baseline_marker_id

        df_idx_film1 = df.index[df['marker_ed'] == '#* 4'][0]
        df.loc[df_idx_film1:df_idx_film1+120000, 'marker'] = film1_marker_id

        df_idx_film2 = df.index[df['marker_ed'] == '#* 12'][0]
        df.loc[df_idx_film2:df_idx_film2+120000, 'marker'] = film2_marker_id

        df_idx_film3 = df.index[df['marker_ed'] == '#* 20'][0]
        df.loc[df_idx_film3:df_idx_film3+120000, 'marker'] = film3_marker_id

        # to .csv
        df = df[['timestamp', 'meter1', 'ecg1', 'sc1', 'reSYS', 'reDIA', 'CO', 'TPR', 'marker']]
        df = df.rename(columns={'meter1': 'affect', 'ecg1': 'ECG', 'sc1': 'EDA', 'reSYS': 'SBP', 'reDIA': 'DBP'})

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

        header = f"""#Study_name,Study 5
#Subject_ID,{output_subject_id}
#Subject_Age,{subject_age}
#Subject_Sex,{subject_sex}
#Channel_Name,timestamp,affect,ECG,EDA,SBP,DBP,CO,TPR,marker
#Data_Category,timestamp,data,data,data,data,data,data,data,marker
#Data_Unit,second,custom,millivolts,microsiemens,mmHg,mmHg,l/min,mmHg*min/l,string
#Data_Sample_rate,1000Hz,1000Hz,1000Hz,1000Hz,Beat-to-beat,Beat-to-beat,Beat-to-beat,Beat-to-beat,1000Hz
#Data_Device,LabChart 8.19 (ADInsturments, New Zealand),Response Meter (ADInsturments, New Zealand),ECG (ADInsturments, New Zealand),GSR Amp (ADInstruments, New Zealand),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Labchart 8.19 (AdInsturments, New Zealand)
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
    pool = mp.Pool()
    dir_path = 'data/Study 5'
    moca_paths = [os.path.join(dir_path, p) for p in os.listdir(dir_path) if 'MOCA_' in p]
    r = list(tqdm.tqdm(pool.imap(process_path, moca_paths), total=len(moca_paths)))
