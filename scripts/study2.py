import multiprocessing as mp
import os
import pandas as pd
import tqdm
import traceback

co = pd.read_excel('data/condition5.xlsx', 'study2')

st = pd.read_excel('data/condition6.xlsx', 'list of stimuli')

column_names = ['timestamp', 'meter', 'temp', 'GSR', 'EKG', 'SBP', 'DBP', 'TPR', 'CO', 'marker']
dtype_dict = {k: 'float' for k in column_names}
dtype_dict['marker'] = 'string'

baseline_marker_id = '-1'

mi = pd.read_excel('data/missing.xlsx', 'Study2')


def process_path(file_path):
    try:
        filename = os.path.basename(file_path)
        subject_id = [s for s in filename.split(' ') if 'os' in s][0].strip('os')
        subject_id = int(subject_id)

        with open(file_path) as f:
            for i, line in enumerate(f):
                if 'BottomValue' in line:
                    rows_to_skip = i + 1  # for files with multiple headers inside

        df = pd.read_csv(file_path, sep='\t', header=None, skiprows=rows_to_skip, encoding='cp1250', decimal=',',
                         names=column_names, dtype=dtype_dict)

        df = df.drop(columns='temp')
        df['marker'] = df['marker'].apply(lambda x: x.strip() if type(x) == str else x)

        if subject_id in mi['id']:
            missing_value = mi.loc[mi['id'] == subject_id, 'channel'].item()

            if missing_value == 'SBP,DBP,CO, TPR':
                df.loc[:, 'SBP'] = float('nan')
                df.loc[:, 'DBP'] = float('nan')
                df.loc[:, 'CO'] = float('nan')
                df.loc[:, 'TPR'] = float('nan')
                print(filename, 'SBP, DBP')

        subject_sex = int(co.loc[co['lp'] == subject_id, 'płec'].item())
        subject_age = int(co.loc[co['lp'] == subject_id, 'age'].item())

        stress_marker_id = int(co.loc[co['lp'] == subject_id, 'Stres_condition_ANGER208_FEAR209'].item())
        photo_marker_id = int(co.loc[co['lp'] == subject_id, 'Photo_condition_HIGH309_LOW308_NEUT_108'].item())

        stress_marker_name = st[st.iloc[:, 2] == stress_marker_id].iloc[:, 4].item()
        photo_marker_name = st[st.iloc[:, 2] == photo_marker_id].iloc[:, 4].item()

        df_idx_m1 = df.index[df['marker'] == '#* 1'][0]
        df.loc[df_idx_m1:df_idx_m1+300000, 'final_marker'] = baseline_marker_id

        df_idx_m2 = df.index[df['marker'] == '#* 2'][0]
        df.loc[df_idx_m2:df_idx_m2+180000, 'final_marker'] = photo_marker_id

        df_idx_m20 = df.index[df['marker'] == '#* 20'][0]
        df.loc[df_idx_m20:df_idx_m20+180000, 'final_marker'] = stress_marker_id

        df = df.rename(columns={'EKG': 'ECG', 'GSR': 'EDA'})
        df = df[['timestamp', 'meter', 'ECG', 'EDA', 'SBP', 'DBP', 'CO', 'TPR', 'final_marker']]
        df = df.rename(columns={'final_marker': 'marker', 'meter': 'affect'})

        for c in ['timestamp']:
            df.loc[:, c] = df[c].map(lambda x: '%.10g' % x)

        for c in ['ECG', 'EDA']:
            df.loc[:, c] = df[c].map(lambda x: '%.3f' % x)

        header = f"""#Study_name,Study 2
#Subject_ID,{subject_id}
#Subject_Age,{subject_age}
#Subject_Sex,{subject_sex}
#Channel_Name,timestamp,affect,ECG,EDA,SBP,DBP,CO,TPR,marker
#Data_Category,timestamp,data,data,data,data,data,data,data,marker
#Data_Unit,second,custom,millivolts,volts,mmHg,mmHg,l/min,mmHg*min/l,string
#Data_Sample_rate,1000Hz,1000Hz,1000Hz,1000Hz,Beat-to-beat,Beat-to-beat,Beat-to-beat,Beat-to-beat,1000Hz
#Data_Device,LabChart 8.19 (ADInsturments, New Zealand),Response Meter (ADInsturments, New Zealand),ECG (ADInsturments, New Zealand),GSR Amp (ADInstruments, New Zealand),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Finometer NOVA (Finapres Medical Systems, Netherlands),Labchart 8.19 (AdInsturments, New Zealand)
"""

        df_baseline = df[df_idx_m1:df_idx_m1 + 300000]

        with open(f'{subject_id}_Baseline.csv', 'w', newline='') as f:
            f.write(header)
            df_baseline.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

            df_photo = df[df_idx_m2:df_idx_m2 + 180000]

        with open(f'{subject_id}_{photo_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_photo.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

            df_stress = df[df_idx_m20:df_idx_m20 + 180000]

        with open(f'{subject_id}_{stress_marker_name}.csv', 'w', newline='') as f:
            f.write(header)
            df_stress.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

        with open(f'{subject_id}_All.csv', 'w', newline='') as f:
            f.write(header)
            df.reset_index(drop=True).to_csv(f, index=False, line_terminator='\n')

    except Exception as e:
        print(file_path, '\n', repr(e), '\n', traceback.format_exc())

    return 0


if __name__ == '__main__':
    pool = mp.Pool()
    dir_path = 'data/Study 2'
    file_paths = [os.path.join(dir_path, p) for p in os.listdir(dir_path)]
    r = list(tqdm.tqdm(pool.imap(process_path, file_paths), total=len(file_paths)))
