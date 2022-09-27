import os
import numpy as np
import yaml
import requests

with open('./config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

__path_seed_data__ = config['files']['path_seed_data']
__path_chain_data__ = config['files']['path_chain_data']
__path_offchain_data__ = config['files']['path_offchain_data']
__path_preprocess_data__ = config['files']['path_preprocess_data']
__path_building_block_data__ = config['files']['path_building_block_data']
__path_tables__ = config['tables']['path_tables']
__path_figures__ = config['figures']['path_figures']


def getFileWithExt(path, ext=".csv"):
    csv_files = []
    for root, dirs, files in os.walk(os.path.realpath(path)):
        for file in files:
            if (file.endswith(ext)):
                csv_files.append(os.path.join(root, file))
    return csv_files

def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)
        os.chmod(path, 0o777)

def df2csv(df,fn):
    df.to_csv(fn)
    os.chmod(fn, 0o777)

def df2csvgz(df,fn):
    df.to_csv(fn, compression="gzip")
    os.chmod(fn, 0o777)

# extract block range of filename e.g. trace_13209000-13209999.csv.gz -> (13209000,13209999)
def fn2blockrange(fn):
    r = os.path.basename(fn).split(".",1)[0].strip("trace_").split("_")[0].split("-")
    if(len(r)==2):
        if(all(list(map(lambda x: str(x).isnumeric(),r)))):
            return(tuple(map(lambda y: int(y),r)))
    return(tuple([np.nan,np.nan]))

# compare if ranges overlap
def range_intersect(r1, r2):
    return np.max([np.min(r1), np.min(r2)]) <= np.min([np.max(r1), np.max(r2)])

# x = pd.interval_range(start = pd.Timestamp("2020-01-01"), end = pd.Timestamp("2022-01-01"),freq=pd.tseries.offsets.DateOffset(months=1))
# y = pd.DataFrame(data = {'start':x.left, 'end':x.right})
#y["block_range"] = y.apply(lambda t: f"{ethscan_block_from_ts(apikey, t['start'].timestamp())}-{+ethscan_block_from_ts(apikey, t['end'].timestamp())}", axis = 1)
