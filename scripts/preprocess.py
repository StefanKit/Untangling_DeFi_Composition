import scripts.basicFun as bf
import os as os
import pandas as pd
import time
import operator
from web3 import Web3
from multiprocessing import Pool, cpu_count
from joblib import Parallel, delayed
import glob as glob

w3 = Web3(Web3.HTTPProvider('http://192.168.243.2:28545'))

path_seed_data = bf.__path_seed_data__
path_chain_data = bf.__path_chain_data__
path_offchain_data = bf.__path_offchain_data__
path_preprocess_data = bf.__path_preprocess_data__
path_tables = bf.__path_tables__
path_figures = bf.__path_figures__


def extractContractCreations(block_range):
    # get all file pathes for separate traces
    chain_files = glob.glob(os.path.abspath(path_chain_data) + "/**/*.*", recursive=True)
    chain_files.sort()

    # trace files that ends with csv.gz or csv.gz.orig
    trace_files = list(filter(lambda f: os.path.basename(f).startswith("trace_") &
                                        (os.path.basename(f).endswith(".csv.gz") |
                                         os.path.basename(f).endswith(".csv.gz.orig") ), chain_files))

    tic = time.perf_counter()  # take initial time

    bf.mkdir(os.path.join(path_preprocess_data, 'contract_creations'))
    l_create = []

    # for each trace file extract contract creations
    for trace_file in trace_files:
        df_trace_sep = pd.read_csv(trace_file, compression='gzip', dtype={'value': 'float'}, low_memory=False).\
            rename(columns={"block_id":"block_number", "tx_hash":"transaction_hash"})

        df_create = df_trace_sep.loc[df_trace_sep.trace_type == "create",
                                     ["block_number", "from_address", "to_address","output"]]

        # check if they are ERC20 compatible i.e. have an ERC20 token interface
        def isERC20(contractBytecode):
            return (all((method in contractBytecode) for method in
                        ["18160ddd", "70a08231", "a9059cbb", "23b872dd", "095ea7b3", "dd62ed3e", "ddf252ad",
                         "8c5be1e5"]))

        df_create.loc[:,"isERC20"] = df_create["output"].apply(str).apply(isERC20)

        l_create.append(df_create[["block_number", "from_address", "to_address","isERC20"]])

        toc = time.perf_counter()
        print(f"Performed the task {trace_file} in {toc - tic:0.4f} seconds")
        tic = time.perf_counter()

    ## create one single creation and contract file and concate it with the previous ones
    df_create_all = pd.concat(l_create, ignore_index= True)
    df_create_all = df_create_all[~(df_create_all.to_address.isna() | df_create_all.from_address.isna())]

    # only use contract creations until the end of the block-range
    df_create_all = df_create_all.loc[(df_create_all.block_number <= max(block_range))]

    # store contract creations from traces
    bf.df2csvgz(df = df_create_all[['block_number', 'from_address', 'to_address','isERC20']],
              fn = os.path.join(path_preprocess_data, 'contract_creations',"trace_creations.csv.gz"))

    # separete ERC20 contracts
    df_contracts_ERC20 = df_create_all[df_create_all.isERC20 == True] # filter ERC20 tokens
    # filter double entries and extract non-ERC20 Tokens
    df_contracts_noneERC20 = df_create_all[~df_create_all.to_address.isin(df_contracts_ERC20.to_address)]
    df_contracts = pd.concat([
        pd.DataFrame({'address': (df_contracts_ERC20.to_address.unique()), 'type': 1, 'isERC20': True}),
        pd.DataFrame({'address': (df_contracts_noneERC20.to_address.unique()), 'type': 1, 'isERC20': False})
    ], ignore_index=True)
    # store each contract and its ERC20 compatibility
    bf.df2csvgz( df = df_contracts[['address', 'type','isERC20']],
                fn = os.path.join(path_preprocess_data, 'contract_creations', "contracts.csv.gz"))

    print("contract creation files: finished")

def processMethodSignatures():
    data = pd.read_csv(path_offchain_data + "/ethereum.functionsignatures.csv")
    print(f"data shape: {data.shape}")
    mdict = {key: dict() for key in list(data.selector.unique())}
    print(f"length of unique data:{len(mdict)}")
    for index, row in data.iterrows():
        sign = row['signature'].split("(")[0]
        if sign not in list(mdict[row['selector']].keys()):
            mdict[row['selector']][sign] = 1
        else:
            mdict[row['selector']][sign] += 1

    for k in mdict.keys():
        if len(mdict[k]) > 1:
            sdict = dict(sorted(mdict[k].items(), key=operator.itemgetter(1), reverse=True))
            mdict[k] = list(sdict.keys())[0]
        else:
            mdict[k] = list(mdict[k].keys())[0]
    df = pd.DataFrame.from_dict({'selector': list(mdict.keys()), 'method': list(mdict.values())})
    fn = os.path.join(path_preprocess_data, 'method_signatures.csv')
    df.to_csv(fn, index=False)
    os.chmod(fn, 0o777)
    print("Method processMethodSignatures finished")

def createProtocolAddresses():
    def getProAddr(fpath):
        path = os.path.normpath(fpath)
        ps = path.split(os.sep)
        if (len(ps) >= 3):
            pt, p, fn = ps[-3:]  # ... protocol_type / protocol / filename
            if (fn == ("sc-" + p + ".csv")):  # if aggregated edges
                # read csv; rename; add dir name info; correct hex values to lower case
                df_sc_addr = pd.read_csv(fpath, encoding='latin1') \
                    .rename(columns={"Type": "asset_type", "Label": "label"}) \
                    .assign(protocol=p, protocol_type=pt, path=path) \
                    .assign(address=lambda x: x.Id.apply(str.lower))
                return df_sc_addr
        return pd.DataFrame({})

    csv_files = bf.getFileWithExt(os.path.join(path_seed_data, 'protocols'))

    df_ProAddr = pd.concat(map(getProAddr, csv_files), ignore_index=True, axis=0)

    # reduce columns and write to csv
    fn = os.path.join(path_preprocess_data, 'Protocol_Addresses.csv')
    # column subset
    col_sub = list(set(df_ProAddr.columns).intersection(set(['address', 'label', 'protocol', 'protocol_type'])))
    df_ProAddr[col_sub].to_csv(fn)
    os.chmod(fn, 0o777)
    print("create Protocol Addresses finished")

def extandProtocolAddresses():
    contract_creations = pd.read_csv(os.path.join(path_preprocess_data, 'contract_creations', 'trace_creations.csv.gz'), compression='gzip').\
        drop("Unnamed: 0", axis = 1, errors='ignore')

    addr = pd.read_csv(os.path.join(path_preprocess_data, 'Protocol_Addresses.csv')).drop("Unnamed: 0", axis = 1)

    # drop duplicated entries
    contract_creations.drop_duplicates(inplace=True)
    # ensure there are no empty (failed) creations
    contract_creations = contract_creations[~contract_creations.to_address.isnull()]
    # get creations from labeled addresses to unlabeled addresses
    unlabeled_creations = contract_creations[contract_creations.from_address.isin(addr.address)]
    del (contract_creations)  # we don't need this anymore# favour created labels over seed data

    # favour created labels over seed data and therefore remove seed entries
    oldLabels = addr[~addr.address.isin(unlabeled_creations.to_address)]\
        .assign(prev_label = "")


    # propagate labels from original contract and keep previous labels, if they exist
    newLabels = unlabeled_creations[['block_number', 'from_address', 'to_address']]\
        .merge(addr.rename(columns={"address": "from_address"}), on="from_address") \
        .merge(addr.assign(prev_label=lambda x: x.label + "_" + x.protocol)[["address", "prev_label"]]\
                    .rename(columns={"address": "to_address"}), on="to_address", how="left")
    # label them "[Source Label]_DEPLOYED"
    newLabels["label"] = newLabels["label"] + "_DEPLOYED"
    # only use unique entries of newLabel (some creations are in multiple blocks - failed/worked)
    reducedNewLabels = newLabels[['to_address', 'protocol', 'protocol_type', 'label','prev_label']] \
                            .drop_duplicates()\
                            .rename(columns={'to_address': 'address'})


    # use original addresses and newly found contract addresses by creation
    # combine new- and old-addresses into one expanded protocol address file
    bf.df2csvgz(df=pd.concat([oldLabels.assign(origin = "seed"),
                            reducedNewLabels.assign(origin = "deployed")]),
                fn = os.path.join(path_preprocess_data, 'Protocol_Addresses_expanded.csv.gz'))
    print("Expanded Protocol Addresses finished")

def filterFiles(file_numbers, block_range):
    # get all file pathes for separate traces

    chain_files = glob.glob(os.path.abspath(path_chain_data) + "/**/*.*", recursive=True)
    chain_files.sort()

    # trace files that ends with csv.gz or csv.gz.orig
    trace_files = list(filter(lambda f: os.path.basename(f).startswith("trace_") &
                                        (os.path.basename(f).endswith(".csv.gz") |
                                         os.path.basename(f).endswith(".csv.gz.orig")), chain_files))

    # filter for trace files in reference block-range
    trace_files = list(filter(lambda f: bf.range_intersect(bf.fn2blockrange(f), block_range), trace_files))

    # load ethereum method signatures
    ethSig = pd.read_csv(os.path.join(path_preprocess_data, 'method_signatures.csv')) \
        .rename(columns={'selector': 'MethodId', 'method': 'MethodName'})

    # load smart contract addresses of protocols
    addr_expanded = pd.read_csv(os.path.join(path_preprocess_data, 'Protocol_Addresses_expanded.csv.gz'),
                                dtype = {'prev_label': str}, compression='gzip')
    addr_expanded['address'] = addr_expanded.address.apply(str.lower)

    if (file_numbers < len(trace_files)):
        trace_files = trace_files[:file_numbers]

    tic = time.perf_counter()  # take initial time

    bf.mkdir(os.path.join(path_preprocess_data, 'filtered_traces'))
    for trace_file in trace_files:
        df_trace_sep = pd.read_csv(trace_file, compression='gzip').\
            rename(columns={"block_id":"block_number", "tx_hash":"transaction_hash"})

        # extract external Transaction
        df_sep_extTrans = df_trace_sep[df_trace_sep.trace_address.isna()]  # ext Transactions don't have a trace address
        # filter ext Tr for protocol addresses
        df_sep_extTrans_fil = df_sep_extTrans[df_sep_extTrans.to_address.isin(addr_expanded.address)].loc[:,
                              ['to_address', 'transaction_hash']].merge(
            addr_expanded[["address","protocol","label"]]\
                .rename(columns={'address': 'to_address', 'protocol': 'ext_protocol', 'label': 'ext_label'}),
            on='to_address', how='left')

        # filter entire traces with filtered ext transaction
        df_sep_fil = df_trace_sep[df_trace_sep.transaction_hash.isin(df_sep_extTrans_fil.transaction_hash)]

        # join protocol infos corresponding to 'transaction_hash', 'to_address' and 'from_address'
        df_sep_fil_pro = df_sep_fil.merge(
            df_sep_extTrans_fil.drop('to_address', axis=1), on='transaction_hash', how='left').merge(
            addr_expanded[["address","protocol","label"]]\
                .rename(columns={'address': 'to_address', 'protocol': 'to_protocol', 'label': 'to_label'}),
                on='to_address', how='left').merge(
            addr_expanded[["address","protocol","label"]]\
                .rename(columns={'address': 'from_address', 'protocol': 'from_protocol', 'label': 'from_label'}),
                on='from_address', how='left')

        # add method signature from input sequence
        df_sep_fil_pro['MethodId'] = df_sep_fil_pro.input.apply(lambda x: str(x)[0:10])
        df_sep_fil_pro = df_sep_fil_pro.merge(ethSig, on="MethodId", how="left")

        fn = os.path.join(path_preprocess_data, 'filtered_traces',
                          os.path.basename(trace_file).split(".",1)[0] + '_fil.' +
                          os.path.basename(trace_file).split(".",1)[1])
        # drop in- and output sequencey because they require a lot of space
        bf.df2csvgz(df = df_sep_fil_pro.drop(['input', 'output'], axis=1), fn = fn)


        toc = time.perf_counter()
        print(f"Performed the task {fn} in {toc - tic:0.4f} seconds")
        tic = time.perf_counter()

    print("filter files: finished")

def aggFiles(block_range):
    columnsToAgg = ['from_address', 'to_address', 'transaction_type', 'to_label', 'to_protocol', 'from_label',
                    'from_protocol']

    trace_fil_files = glob.glob(os.path.join(os.path.abspath(path_preprocess_data),"filtered_traces") + "/**/*.csv.gz", recursive=True)
    trace_fil_files.sort()

    # filter for trace files in reference block-range
    trace_fil_files = list(filter(lambda f: bf.range_intersect(bf.fn2blockrange(f), block_range), trace_fil_files))

    l_agg = []
    # aggregate all filtered files
    ## therefore groupby selected columns and count edges
    bf.mkdir(os.path.join(path_preprocess_data, 'agg_traces'))
    for trace_fil_file in trace_fil_files:
        tic = time.perf_counter()
        df_traces = pd.read_csv(trace_fil_file, dtype={'value': 'float'}, compression='gzip')

        df_traces = df_traces.loc[(df_traces.block_number >= min(block_range)) &
                                  (df_traces.block_number <= max(block_range))]

        # define transaction_type as external- and internal-transactions
        df_traces['transaction_type'] = df_traces.trace_id.apply(
            lambda s: 'external' if (str(s).endswith('_')) else 'internal')

        df_count = df_traces.groupby(
            columnsToAgg, dropna=False) \
            .trace_id.count() \
            .reset_index().rename(columns={'trace_id': 'transaction_count'})  #

        l_agg.append(df_count)

        toc = time.perf_counter()
        print(f"Performed the agg-task {trace_fil_file} in {toc - tic:0.4f} seconds")

    if(len(l_agg)>0):

        # create all-in-one file
        fn = os.path.join(path_preprocess_data, 'agg_traces',
                          'traces_' + str(block_range[0]) + "-" + str(block_range[1])+ '_agg.csv.gz')
        # list(map(lambda c: pd.read_csv(c), csv_files))
        df_agg = pd.concat(l_agg, ignore_index=True) \
            .groupby(
            columnsToAgg, dropna=False) \
            .transaction_count.sum() \
            .reset_index()

        bf.df2csvgz(df = df_agg, fn = fn)

        print("agg files: finished")

def get_CA_network(block_range):

    # load contracts and agg traces
    cas = pd.read_csv(os.path.join(path_preprocess_data, 'contract_creations', "contracts.csv.gz"),
                      dtype={'isERC20': str}, compression='gzip')

    fn_traces = os.path.join(path_preprocess_data, 'agg_traces',
                             'traces_' + str(block_range[0]) + "-" + str(block_range[1]) + '_agg.csv.gz')

    if(os.path.exists(fn_traces)):

        traces = pd.read_csv(fn_traces, compression='gzip')

        # remove index colum
        traces = traces[['from_address', 'to_address', 'transaction_type', 'to_label', 'to_protocol', 'from_label',
                         'from_protocol','transaction_count']]

        # reduce network by filtering only CA-addresses
        ca_network = traces[(traces.from_address.isin(cas.address)) & (traces.to_address.isin(cas.address))]
        # drop the na-entries (which where only for check-reasons)
        ca_network = ca_network[(~ca_network.to_address.isna()) & (~ca_network.from_address.isna())]

        # store defi ca network
        bf.mkdir(os.path.join(path_preprocess_data, 'defi_networks'))
        bf.df2csvgz(df = ca_network,
                    fn = os.path.join(path_preprocess_data, 'defi_networks','defi_ca_network_' +
                                      str(block_range[0]) + "-" + str(block_range[1]) + '.csv.gz'))

        print("ca network: finished")

    else:

        print("ca network: failed, agg-file doesn't exist!")

def get_CA_network_pro(block_range):

    fn_ca_network = os.path.join(path_preprocess_data, 'defi_networks', 'defi_ca_network_' +
                 str(block_range[0]) + "-" + str(block_range[1]) + '.csv.gz')

    if(os.path.exists(fn_ca_network)):

        ca_network = pd.read_csv(fn_ca_network, compression='gzip').drop(columns=['Unnamed: 0'])

        # create columns which aggregates all addresses of protocol
        ca_network['from_address_AggPro'] = ca_network['from_address']
        ca_network.loc[~ca_network.from_protocol.isna(), 'from_address_AggPro'] = ca_network.loc[
            ~ca_network.from_protocol.isna(), 'from_protocol']
        ca_network['to_address_AggPro'] = ca_network['to_address']
        ca_network.loc[~ca_network.to_protocol.isna(), 'to_address_AggPro'] = ca_network.loc[
            ~ca_network.to_protocol.isna(), 'to_protocol']

        # aggregate protocols
        ca_network_pro = ca_network.groupby(['from_address_AggPro', 'to_address_AggPro', 'transaction_type',
                                             'from_protocol', 'to_protocol'],
                                      dropna=False).transaction_count.sum().reset_index()
        ca_network_pro.rename(columns = {'from_address_AggPro':'from_address',
                                         'to_address_AggPro':'to_address'}, inplace = True)

        bf.mkdir(os.path.join(path_preprocess_data, 'defi_networks'))

        bf.df2csvgz(df = ca_network_pro,
                    fn = os.path.join(path_preprocess_data, 'defi_networks', 'defi_ca_network_pro_' +
                 str(block_range[0]) + "-" + str(block_range[1]) + '.csv.gz'))

        print("pro network: finished")

    else:

        print("pro network: failed, ca network doesn't exist!")

def get_addr_summary(outpath = os.path.join(path_tables,"seed_addresses.tex")):
    addr = pd.read_csv(os.path.join(path_preprocess_data, 'Protocol_Addresses.csv'))

    addr_expanded = pd.read_csv(os.path.join(path_preprocess_data, 'Protocol_Addresses_expanded.csv.gz'),
                                dtype={'prev_label': str})
    addr_expanded['address'] = addr_expanded.address.apply(str.lower)

    gr_ad = addr.groupby(["protocol_type", "protocol"]).count()['label'].reset_index()
    gr_ad_ext = addr_expanded.groupby(["protocol_type", "protocol"]).count()['label'].reset_index()

    df_seed = pd.merge(gr_ad, gr_ad_ext, on=["protocol_type", "protocol"], how="left") \
        .rename(columns={'protocol_type': 'type', 'label_x': 'seed', 'label_y': 'seed extended'})


    # create seed table with number of addresses
    if(not(pd.isnull(outpath))):
        with open(outpath, "w") as text_file:
            text_file.write(
                df_seed \
                    .to_latex(index=False,
                              column_format=r'>{\centering\arraybackslash}p{0.1\textwidth}'
                                            r'>{\centering\arraybackslash}m{0.1\textwidth}'
                                            r'>{\centering\arraybackslash}m{0.07\textwidth}'
                                            r'>{\centering\arraybackslash}p{0.13\textwidth}') \
                    .replace(r"\toprule", r"\toprule  &  & \multicolumn{2}{c}{number of addresses}  \\ \cline{3-4} ")
            )

    return df_seed


def get_tvl(outpath):
    l_df = []
    for file in glob.glob(path_offchain_data + "/defipulse_**.csv"):
        df_tvl_table = pd.read_csv(file).drop("Unnamed: 0", axis=1)
        df_tvl_table.loc[:, "tvl"] = pd.to_numeric(df_tvl_table.loc[:, "Locked (USD)"].apply(lambda s: s[1:-1])). \
            mul(df_tvl_table.loc[:, "Locked (USD)"].apply(lambda s: s[-1]).map({'B': 1e9, 'M': 1e6, 'K': 1e3}))
        df_tvl_table.loc[:, "date"] = os.path.basename(file).split("_")[1].split(".")[0][0:8]
        df_tvl_table = df_tvl_table.loc[~(df_tvl_table.Name == "TVL")]

        l_df.append(df_tvl_table)
    df_tvl = pd.concat(l_df, ignore_index=True).pivot(index=["Name", "Category"], columns=["date"],
                                                      values="tvl").reset_index()  # .to_csv("defipulse_tvl.csv")
    df_tvl = df_tvl.reset_index(drop=True).set_index(["Name", "Category"]).sum(axis=1, skipna=True)
    df_tvl = df_tvl / df_tvl.sum() * 100

    if(not(pd.isnull(outpath))):
        df_tvl.to_csv(outpath)

    return(df_tvl)

