import multiprocessing as mp
import os
import pandas as pd
import tqdm
import traceback

co = pd.read_excel('data/condition5.xlsx', 'study1')

st = pd.read_excel('data/condition6.xlsx', 'list of stimuli')

column_names = ['timestamp', 'meter', 'temp', 'BP1', 'BP2', 'resp2', 'GSR', 'EKG', 'marker']
dtype_dict = {k: 'float' for k in column_names}
dtype_dict['marker'] = 'string'

baseline_marker_id = '-1'
stress_marker_id = '209'
stress_marker_name = 'Threat'

mi = pd.read_excel('data/missing.xlsx', 'Study1')


def process_path(file_path):
    try:
        filename = os.path.basename(file_path)

        df = pd.read_csv(file_path, sep='\t', header=None, skiprows=9, decimal=',', names=column_names, dtype=dtype_dict)

        df['marker'] = df['marker'].apply(lambda x: x.strip() if type(x) == str else x)

        subject_id = int([s for s in filename.split(' ') if 'osoba' in s][0].strip('osoba'))

        if subject_id in mi['id']:
            missing_value = mi.loc[mi['id'] == subject_id, 'channel'].item()

            if missing_value == 'resp':
                df.loc[:, 'resp2'] = float('nan')
                print(filename, 'resp')

            elif missing_value == 'SBP':
                df.loc[:, 'BP1'] = float('nan')
                df.loc[:, 'BP2'] = float('nan')
                print(filename, 'SBP, DBP')

        subject_sex = int(co.loc[co['Subject_ID'] == subject_id, 'płec'].item())
        photo_marker_id = int(co.loc[co['Subject_ID'] == subject_id, 'condition'].item())

        photo_marker_name = st[st.iloc[:, 2] == photo_marker_id].iloc[:, 4].item()

        try:
            subject_age = int(co.loc[co['Subject_ID'] == subject_id, 'wiek'].item())
        except Exception:
            subject_age = 0

        df_idx_m1 = df.index[df['marker'] == '#* 1'][0]
        df.loc[df_idx_m1:df_idx_m1 + 300000, 'final_marker'] = baseline_marker_id

        df_idx_m2 = df.index[df['marker'] == '#* 2'][0]
        df.loc[df_idx_m2:df_idx_m2 + 30000, 'final_marker'] = stress_marker_id

        df_idx_m101 = df.index[df['marker'] == '#* 101'][0]
        df.loc[df_idx_m101:df_idx_m101 + 180000, 'final_marker'] = photo_marker_id

        header = f"""#Study_name,Study 1
#Subject_ID,{subject_id}
#Subject_Age,{subject_age}
#Subject_Sex,{subject_sex}
#Channel_Name,timestamp,affect,ECG,EDA,temp,respiration,SBP,DBP,marker
#Data_Category,timestamp,data,data,data,data,data,data,data,marker
#Data_Unit,second,custom,millivolts,microsiemens,Celsius,millivolts,mmHg,mmHg,string
#Data_Sample_rate,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz,1000Hz
#Data_Device,LabChart 8.19 (ADInsturments, New Zealand),Response Meter (ADInsturments, New Zealand),ECG (ADInsturments, New Zealand),GSR Amp (ADInstruments, New Zealand),Thermistor Pod (ADInstruments, New Zealand),Pneumotrace II (UFI, USA),Finometer MIDI (Finapres Medical Systems, Netherlands),Finometer MIDI  (Finapres Medical Systems, Netherlands),Labchart 8.19 (AdInsturments, New Zealand)
"""

        df.loc[:, 'timestamp'] = df['timestamp'].map(lambda x: '%.10g' % x)
        df.loc[:, 'EKG'] = df['EKG'].map(lambda x: '%.3f' % x)

        df = df.rename(columns={'resp2': 'respiration', 'GSR': 'EDA', 'EKG': 'ECG', 'BP1': 'SBP', 'BP2': 'DBP'})
        df = df[['timestamp', 'meter', 'ECG', 'EDA', 'temp', 'respiration', 'SBP', 'DBP', 'final_marker']]
        df = df.rename(columns={'final_marker': 'marker', 'meter': 'affect'})

        df_baseline = df[df_idx_m1:df_idx_m1 + 300000]
        with open(f'{subject_id}_Baseline.csv', 'w', newline='') as f:
            f.write(header)
            df_baseline.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_stress = df[df_idx_m2:df_idx_m2 + 30000]
        with open(f'{subject_id}_{stress_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_stress.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        df_photo = df[df_idx_m101:df_idx_m101 + 180000]
        with open(f'{subject_id}_{photo_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_photo.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        with open(f'{subject_id}_All.csv', 'w', newline='') as f:
            f.write(header)
            df.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

    except Exception as e:
        print(file_path, '\n', repr(e), '\n', traceback.format_exc())

    return 0


if __name__ == '__main__':
    pool = mp.Pool()
    dir_path = 'data/Study 1'
    file_paths = [os.path.join(dir_path, p) for p in os.listdir(dir_path)]
    r = list(tqdm.tqdm(pool.imap(process_path, file_paths), total=len(file_paths)))
