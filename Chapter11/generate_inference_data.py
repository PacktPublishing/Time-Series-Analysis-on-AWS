import argparse
import datetime
import os
import pandas as pd
import pytz

def main():
    os.makedirs('inference-data/input', exist_ok=True)

    # How many sequences of data we want to extract:
    num_sequences = 3

    # The scheduling frequency in minutes: this **MUST** match the
    # resampling rate used to train the model:
    frequency = 5

    # Set current timezone to UTC:
    utc_timezone = pytz.timezone("UTC")

    for root, dirs, files in os.walk('train-data'):
        for f in files:
            component = root.split('/')[-1]
            print(f'Creating inference data from component {component}')

            component_fname = os.path.join(root, f)
            inference_df = pd.read_csv(component_fname)
            inference_df['Timestamp'] = pd.to_datetime(inference_df['Timestamp'])
            inference_df = inference_df.set_index('Timestamp')

            # We know that some events of interest are happening after this date:
            start = pd.to_datetime('2018-12-27 02:05:00')
            for i in range(num_sequences):
                end = start + datetime.timedelta(minutes=+frequency - 1)
                inference_input = inference_df.loc[start:end, :]
                start = start + datetime.timedelta(minutes=+frequency)

                # Rounding time to the previous X minutes 
                # where X is the selected frequency:
                filename_tm = datetime.datetime.now(utc_timezone)
                filename_tm = filename_tm - datetime.timedelta(
                    minutes=filename_tm.minute % frequency,
                    seconds=filename_tm.second,
                    microseconds=filename_tm.microsecond
                )
                filename_tm = filename_tm + datetime.timedelta(minutes=+frequency * (i))
                current_timestamp = (filename_tm).strftime(format='%Y%m%d%H%M%S')

                # The timestamp inside the file are in UTC and are not linked to the current timezone:
                timestamp_tm = datetime.datetime.now(utc_timezone)
                timestamp_tm = timestamp_tm - datetime.timedelta(
                    minutes=timestamp_tm.minute % frequency,
                    seconds=timestamp_tm.second,
                    microseconds=timestamp_tm.microsecond
                )
                timestamp_tm = timestamp_tm + datetime.timedelta(minutes=+frequency * (i))

                # We need to reset the index to match the time 
                # at which the scheduler will run inference:
                new_index = pd.date_range(
                    start=timestamp_tm,
                    periods=inference_input.shape[0], 
                    freq='1min'
                )
                inference_input.index = new_index
                inference_input.index.name = 'Timestamp'
                inference_input = inference_input.reset_index()
                inference_input['Timestamp'] = inference_input['Timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

                # Export this file in CSV format:
                scheduled_fname = os.path.join('inference-data', 'input', f'{component}_{current_timestamp}.csv')
                inference_input.to_csv(scheduled_fname, index=None)
            
if __name__ == '__main__':
    main()