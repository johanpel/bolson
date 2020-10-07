import subprocess
import os
import argparse

parser = argparse.ArgumentParser(description='Sweep over a batches with tweet counts 2^0 .. 2^N')
parser.add_argument('N', type=int, nargs=1)
args = parser.parse_args()

N = args.N[0] + 1

jsongen = '../release/cpp/jsongen/jsongen'
bolson = '../release/cpp/bolson/bolson'

with open('single_batch_sweep_result.csv', 'w') as f:
    f.write("Load JSON (s),"
            "Load JSON (GB/s),"
            "Parse JSON (s),"
            "Parse JSON (GB/s),"
            "Convert to Arrow RecordBatches (in) (s),"
            "Convert to Arrow RecordBatches (in) (GB/s),"
            "Convert to Arrow RecordBatches (out) (s),"
            "Convert to Arrow RecordBatches (out) (GB/s),"
            "Write Arrow IPC messages (s),"
            "Write Arrow IPC messages (GB/s),"
            "Publish IPC messages in Pulsar (s),"
            "Publish IPC messages in Pulsar (GB/s),"
            "JSON File size (B),"
            "Number of tweets,"
            "Number of RecordBatches,"
            "Arrow RecordBatches total size (B),"
            "Arrow RecordBatch avg. size (B),"
            "Arrow IPC messages total size (B),"
            "Arrow IPC messages avg. size (B)\n")
    f.flush()

    for i in range(0, N):
        num_tweets = 2 ** i
        json_file = 'random_{:08d}.json'.format(num_tweets)

        # Run jsongen
        subprocess.run([jsongen,
                        '-s', '42',
                        '-o', json_file,
                        '-n', str(num_tweets)])

        # Run bolson
        process = subprocess.run([bolson, 'prod', '-s', json_file, '-m', str(5 * 1024 * 1024 - 20 * 1024)], stdout=f)
        # Message size is limited to 5 * 1024 * 1024 - 20 * 1024.
        #   Default in Pulsar is 5 * 1024 * 1024 - 10 * 1024, but give some margin for referenced tweets array in a
        #   json tweet object to grow large.

        # Remove file
        os.remove(json_file)

    f.close()
