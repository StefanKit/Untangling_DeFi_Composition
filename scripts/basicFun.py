import os
import yaml

with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

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