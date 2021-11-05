import basicFun as bf
import os as os
import pandas as pd
from argparse import ArgumentParser
import time
import operator
import traceback
from web3 import Web3
import numpy as np
import json
import math
from multiprocessing import Pool, cpu_count
from joblib import Parallel, delayed
import hashlib
import re
import subprocess as sp

path_seed_data = bf.config['files']['path_seed_data']
path_chain_data = bf.config['files']['path_chain_data']
path_preprocess_data = bf.config['files']['path_preprocess_data']
path_tables = bf.config['tables']['path_tables']
path_figures = bf.config['figures']['path_figures']


def applyParallel(dfGrouped, func):
    retLst = Parallel(n_jobs=cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)


def processMethodSignatures():
    data = pd.read_csv(path_chain_data + "/ethereum.functionsignatures.csv")
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


def splitAWK():

    traces_csv = bf.getFileWithExt(path=os.path.join(path_chain_data, 'traces'), ext=".csv")

    bf.mkdir(os.path.join(path_preprocess_data, 'sep_sort_traces'))

    for tr_csv in traces_csv:

        COMMAND = "awk -F, 'NR==1 {h=$0; next} {f=\"" + \
                  os.path.join(path_preprocess_data,"sep_sort_traces",os.path.splitext(os.path.basename(tr_csv))[0] ) + \
                  "_block\" $1%100 \".csv\"}" \
                  " !($1%100 in p) {p[$1%100]; print h > f} {print >> f}' " + \
                  tr_csv

        sp.Popen(COMMAND, stdin=sp.PIPE, stdout=sp.PIPE, shell=True).stdout.read()

    print("spit and sorted files via awk: finished")


def extractContractCreations(file_numbers):
    # get all file pathes for separate traces
    csv_files = bf.getFileWithExt(path=os.path.join(path_preprocess_data, 'sep_sort_traces'), ext=".csv")
    csv_files.sort()

    if (file_numbers < len(csv_files)):
        csv_files = csv_files[:file_numbers]

    tic = time.perf_counter()  # take initial time

    bf.mkdir(os.path.join(path_preprocess_data, 'contract_creations'))
    for csv_file in csv_files:
        df_trace_sep = pd.read_csv(csv_file)

        df_create = df_trace_sep.loc[df_trace_sep.trace_type == "create",
                                     ["block_number", "from_address", "to_address","output"]]

        # detect ERC20 tokens
        def isERC20(contractBytecode):
            return (all((method in contractBytecode) for method in
                        ["18160ddd", "70a08231", "a9059cbb", "23b872dd", "095ea7b3", "dd62ed3e", "ddf252ad",
                         "8c5be1e5"]))

        df_create.loc[:,"isERC20"] = df_create["output"].apply(str).apply(isERC20)

        fn = os.path.join(path_preprocess_data, 'contract_creations',
                          os.path.splitext(os.path.basename(csv_file))[0] + '_crea' +
                          os.path.splitext(os.path.basename(csv_file))[1])
        bf.df2csv(fn = fn,
                  df = df_create[["block_number", "from_address", "to_address","isERC20"]])

        toc = time.perf_counter()
        print(f"Performed the task {fn} in {toc - tic:0.4f} seconds")
        tic = time.perf_counter()

    ## create one single creation and contract file and concate it with the previous ones

    # read all created creations
    csv_files = bf.getFileWithExt(path=os.path.join(path_preprocess_data, 'contract_creations'), ext=".csv")
    csv_files.sort()

    # concate all contract creations from traces.csv to a single set and file
    df_create_new = pd.concat(list(map(
        lambda s: pd.read_csv(s).drop('Unnamed: 0', axis = 1), csv_files)), ignore_index= True).drop_duplicates()

    bf.df2csv(df = df_create_new,
              fn = os.path.join(path_preprocess_data, 'contract_creations',"traces_creation_all.csv"))

    # all new contracts from traces.csv
    df_contracts_new_ERC20 = df_create_new[df_create_new.isERC20 == True] # filter ERC20 tokens
    # filter double entries and extract non-ERC20 Tokens
    df_contracts_new_noneERC20 = df_create_new[~df_create_new.to_address.isin(df_contracts_new_ERC20.to_address)]
    df_contracts_new = pd.concat([
        pd.DataFrame({'address': (df_contracts_new_ERC20.to_address.unique()), 'type': 1, 'isERC20': True}),
        pd.DataFrame({'address': (df_contracts_new_noneERC20.to_address.unique()), 'type': 1, 'isERC20': False})
    ], ignore_index=True)
    bf.df2csv( df = df_contracts_new,
                fn = os.path.join(path_preprocess_data, 'contract_creations', "contracts_all.csv"))

    # read existing contracts.csv and trace_creations.csv
    df_contracts_old = pd.read_csv(os.path.join(path_chain_data, 'contracts.csv'))
    df_create_old = pd.read_csv(os.path.join(path_chain_data, 'trace_creations.csv'))

    # extend it with the new ones and remove duplicate entries
    df_create = pd.concat([
            df_create_old[['block_number', 'from_address', 'to_address','isERC20']],
            df_create_new[['block_number', 'from_address', 'to_address','isERC20']]
        ], ignore_index = True).drop_duplicates()
    bf.df2csv(df = df_create, fn = os.path.join(path_preprocess_data, 'trace_creations.csv'))

    # extend it with the new ones and remove duplicate entries
    df_contracts = pd.concat([
        df_contracts_old[['address', 'type','isERC20']],
        df_contracts_new[['address', 'type','isERC20']]
    ], ignore_index=True).drop_duplicates()
    bf.df2csv(df = df_contracts, fn = os.path.join(path_preprocess_data, 'contracts.csv'))

    print("contract creation files: finished")


def extandProtocolAddresses():
    contract_creations = pd.read_csv(os.path.join(path_preprocess_data, 'trace_creations.csv'))

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
    bf.df2csv(df=pd.concat([oldLabels.assign(origin = "seed"),
                            reducedNewLabels.assign(origin = "deployed")]),
                fn = os.path.join(path_preprocess_data, 'Protocol_Addresses_expanded.csv'))
    print("Expanded Protocol Addresses finished")


def filterFiles(file_numbers):
    # get all file pathes for separate traces
    csv_files = bf.getFileWithExt(path=os.path.join(path_preprocess_data, 'sep_sort_traces'), ext=".csv")
    csv_files.sort()

    # load ethereum method signatures
    ethSig = pd.read_csv(os.path.join(path_preprocess_data, 'method_signatures.csv')) \
        .rename(columns={'selector': 'MethodId', 'method': 'MethodName'})

    # load smart contract addresses of protocols
    addr_expanded = pd.read_csv(os.path.join(path_preprocess_data, 'Protocol_Addresses_expanded.csv'),
                                dtype = {'prev_label': str})
    addr_expanded['address'] = addr_expanded.address.apply(str.lower)

    if (file_numbers < len(csv_files)):
        csv_files = csv_files[:file_numbers]

    tic = time.perf_counter()  # take initial time

    bf.mkdir(os.path.join(path_preprocess_data, 'filtered_traces'))
    for csv_file in csv_files:
        df_trace_sep = pd.read_csv(csv_file)

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

        df_sep_fil_pro['MethodId'] = df_sep_fil_pro.apply(lambda x: str(x.input)[0:10], axis=1)
        df_sep_fil_pro = df_sep_fil_pro.merge(ethSig, on="MethodId", how="left")

        fn = os.path.join(path_preprocess_data, 'filtered_traces',
                          os.path.splitext(os.path.basename(csv_file))[0] + '_fil' +
                          os.path.splitext(os.path.basename(csv_file))[1])
        df_sep_fil_pro.drop(['input', 'output'], axis=1).to_csv(fn)
        os.chmod(fn, 0o777)

        toc = time.perf_counter()
        print(f"Performed the task {fn} in {toc - tic:0.4f} seconds")
        tic = time.perf_counter()

    print("filter files: finished")


def aggFiles():
    columnsToAgg = ['from_address', 'to_address', 'transaction_type', 'to_label', 'to_protocol', 'from_label',
                    'from_protocol']

    csv_files = bf.getFileWithExt(path=os.path.join(path_preprocess_data, 'filtered_traces'))
    csv_files.sort()

    # aggregate all filtered files
    ## therefore groupby selected columns and count edges
    bf.mkdir(os.path.join(path_preprocess_data, 'agg_traces'))
    for csv_file in csv_files:
        tic = time.perf_counter()
        df_traces = pd.read_csv(csv_file, dtype={'value': 'float'})

        df_traces['transaction_type'] = df_traces.trace_id.apply(
            lambda s: 'external' if (str(s).endswith('_')) else 'internal')

        df_count = df_traces.groupby(
            columnsToAgg, dropna=False) \
            .trace_id.count() \
            .reset_index().rename(columns={'trace_id': 'transaction_count'})  #
        #

        fn = os.path.join(path_preprocess_data, 'agg_traces',
                          os.path.splitext(os.path.basename(csv_file))[0] + '_agg' +
                          os.path.splitext(os.path.basename(csv_file))[1])
        df_count.to_csv(fn)
        os.chmod(fn, 0o777)
        toc = time.perf_counter()
        print(f"Performed the agg-task {fn} in {toc - tic:0.4f} seconds")

    # get all agg_traces
    csv_files = bf.getFileWithExt(path=os.path.join(path_preprocess_data, 'agg_traces'));
    csv_files.sort()

    # create all-in-one file
    fn = os.path.join(path_preprocess_data, 'agg_traces', 'traces_all_agg.csv')
    df_agg = pd.concat(list(map(lambda c: pd.read_csv(c), csv_files)), ignore_index=True) \
        .groupby(
        columnsToAgg, dropna=False) \
        .transaction_count.sum() \
        .reset_index()
    df_agg.to_csv(fn)
    os.chmod(fn, 0o777)
    print("agg files: finished")


def get_CA_network():

    # load contracts and agg traces
    cas = pd.read_csv(os.path.join(path_preprocess_data, 'contracts.csv'))
    traces = pd.read_csv(os.path.join(path_preprocess_data, 'agg_traces', 'traces_all_agg.csv'))

    # remove index colum
    traces = traces[['from_address', 'to_address', 'transaction_type', 'to_label', 'to_protocol', 'from_label',
                     'from_protocol','transaction_count']]

    # reduce network by filtering only CA-addresses
    ca_network = traces[(traces.from_address.isin(cas.address)) & (traces.to_address.isin(cas.address))]
    # drop the na-entries (which where only for check-reasons)
    ca_network = ca_network[(~ca_network.to_address.isna()) & (~ca_network.from_address.isna())]

    # store defi ca network
    bf.mkdir(os.path.join(path_preprocess_data, 'defi_networks'))
    bf.df2csv(df = ca_network, fn = os.path.join(path_preprocess_data, 'defi_networks','defi_ca_network.csv'))


def get_CA_network_pro():
    ca_network = pd.read_csv(os.path.join(path_preprocess_data, 'defi_networks','defi_ca_network.csv')).drop(
        columns=['Unnamed: 0'])

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
    bf.df2csv(df=ca_network_pro, fn=os.path.join(path_preprocess_data, 'defi_networks', 'defi_ca_network_pro.csv'))

    # use threshold
    #for t in [1e2, 1e3, 1e4]:
    #    aap_threshold = all_agg_pro[(all_agg_pro.transaction_count >= t)]
    #    fn = os.path.join(path_preprocess_data,
    #                      'agg_protocol',
    #                      f"agg_protocol_treshold1e{int(np.log10(t))}.csv")
    #    aap_threshold.to_csv(fn)
    #    os.chmod(fn, 0o777)


def extractTxSubTraces(file_numbers):

    col_sub = ['to_protocol', 'isSubtrace','subtraces_N', 'MethodId', 'MethodName',
               'subtraces', 'subaddresses', 'subaddresses_sub','MethodIds',
               'subtraces_hash', 'subaddresses_sub_hash', 'sub_hash',
               'ext_protocol', 'ext_address','ext_MethodId']

    mId_ERC20 = list(  {'totalSupply':"0x18160ddd", 'balanceOf':'0x70a08231', 'transfer':'0xa9059cbb',
                         'transferFrom':'0x23b872dd', 'approve':'0x095ea7b3', 'allowance':'0xdd62ed3e'}.values())

    cas = pd.read_csv(os.path.join(path_preprocess_data, 'contracts.csv'), dtype={'isERC20': str})

    csv_files = bf.getFileWithExt(path=os.path.join(path_preprocess_data, 'filtered_traces'))
    csv_files.sort()

    if (file_numbers < len(csv_files)):
        csv_files = csv_files[:file_numbers]

    for csv_file in csv_files:

        bf.mkdir(path = os.path.join(path_preprocess_data,'subTraces'))

        df_traces = pd.read_csv(csv_file, dtype={'value': 'float'})
        df_traces = df_traces.sort_values('trace_id')

        df_traces = pd.merge(left=df_traces, right=cas[['address', 'isERC20']].rename(columns={'address': 'to_address'}),
                          how='left', on="to_address")

        def processTrace(df_traces_sameTxHash):

            df_trace_txHash = df_traces_sameTxHash.copy()

            # remove trace_type in trace_id (e.g. call/create)
            df_trace_txHash.loc[:, "trace_id_re"] = df_trace_txHash \
                .apply(lambda sub: sub.trace_id.replace(sub.trace_type + "_", ""), axis=1)

            # order by reduced trace_id to get correct tx-structure
            df_trace_txHash.sort_values("trace_id_re", ascending = True, inplace=True)

            # methodId has ERC20 ids and is in ERC20 CA list -> ASSET
            df_trace_txHash.loc[:, "isAsset"] = df_trace_txHash \
                .apply(lambda st: (st["MethodId"] in mId_ERC20) & (st["isERC20"] == "True"), axis=1)

            # has been deployed
            df_trace_txHash.loc[:, "isDeployed"] = df_trace_txHash.to_label \
                .apply(lambda s: str(s).endswith("_DEPLOYED"))

            # make subaddress abstraction by subtituting Assets and Deployed-Contract
            # copy col df_subtraces.to_address
            df_trace_txHash.loc[:, "subaddresses_sub"] = df_trace_txHash.loc[:, "to_address"]
            # substitute deployed addresses with DEPLOYED and enumerate
            df_trace_txHash.loc[df_trace_txHash.isDeployed == True, "subaddresses_sub"] = \
                df_trace_txHash.loc[df_trace_txHash.isDeployed == True, "to_protocol"].apply(lambda s: s + "-DEPLOYED")
            # substitute deployed addresses with DEPLOYED and enumerate
            df_trace_txHash.loc[df_trace_txHash.isAsset == True, "subaddresses_sub"] = "ASSET"
            # create address list in order of execution

            # store external call only
            tr0 = df_trace_txHash[df_trace_txHash.trace_address.isna()].iloc[0]

            # extract all entries that start to a known protocol and have subtraces
            df_hasSubtraces = df_trace_txHash.loc[(~(df_trace_txHash.to_protocol.isna())) &
                                                   (df_trace_txHash.subtraces > 0) &
                                                   (df_trace_txHash.isAsset == False), :].copy()
            # df_hasSubtraces = df_trace_txHash.loc[(~(df_trace_txHash.to_protocol.isna())) , :].copy()

            # if subtraces exist
            if (len(df_hasSubtraces) > 0):

                # label all intern subtraces which appear within the trace
                df_hasSubtraces.loc[:, "isIntern"] = (~(df_hasSubtraces.loc[:, "trace_address"].isna()))

                # define empty df to collect subtraces of transaction_hash
                df_buildingBlocks = pd.DataFrame()

                # create additional tx-traces, which where building blocks will be removed
                df_trace_txHash_rmBlocks = df_trace_txHash.copy()

                # for loop df_hasSubtraces - bottom up: starting with the last entries
                df_hasSubtraces.sort_values("trace_id_re", ascending=False, inplace=True) # sort by starting with the last
                df_hasSubtraces.reset_index(drop=True, inplace=True)

                for sub_idx, hasSubtrace in df_hasSubtraces.iterrows():

                    # store components to create a building-block
                    dict_newBuildingBlock = dict()

                    trace_id_reduced = hasSubtrace.trace_id_re
                    df_subtraces = df_trace_txHash[df_trace_txHash.trace_id_re \
                        .apply(lambda s: (str(s).find(
                        trace_id_reduced) >= 0))].copy()  # find matching trace_id's and extract their subtraces
                    df_subtraces.sort_values("trace_id_re", ascending=True, inplace=True)
                    df_subtraces.reset_index(inplace=True)

                    def getStringOfSeries(series):
                        return(str('_').join(list(map(str, series.to_list()))))

                    ## extract subtrace-block content and identifiers from subtraces
                    # create list of number of edges
                    subtraces = df_subtraces.subtraces # represent graph-tree structure
                    dict_newBuildingBlock["subtraces_N"] = sum(subtraces) # get number of links/calls
                    dict_newBuildingBlock["subtraces"] = getStringOfSeries(df_subtraces.subtraces) # subtraces identifier
                    # create address list in order of execution
                    dict_newBuildingBlock["subaddresses"] = getStringOfSeries(df_subtraces.to_address) # address list identifier
                    # create methodId list
                    dict_newBuildingBlock["methodIds"] = getStringOfSeries(df_subtraces.MethodId) # method list identifier
                    # create substituted (abstracted) subaddress list
                    dict_newBuildingBlock["subaddresses_sub"] = getStringOfSeries(df_subtraces.subaddresses_sub)

                    # create a combined hash
                    dict_newBuildingBlock["sub_hash"] = '0x' + str(hashlib.sha256(str(str(dict_newBuildingBlock["methodIds"]) + ',' +
                                                             dict_newBuildingBlock["subtraces"] + ',' +
                                                             dict_newBuildingBlock["subaddresses_sub"]).encode('utf-8')).hexdigest())

                    ## remove just current building-block from trace-tx copy
                    # find all subtraces in shrinking trace-tx copy
                    df_subtraces_sub = df_trace_txHash_rmBlocks[df_trace_txHash_rmBlocks.trace_id_re \
                        .apply(lambda s: (str(s).find(trace_id_reduced) >= 0))]
                    # create hash-list of subaddresses_sub and subtraces
                    dict_newBuildingBlock["subaddresses_sub_hash"] = getStringOfSeries(df_subtraces_sub.subaddresses_sub)
                    dict_newBuildingBlock["subtraces_hash"] = getStringOfSeries(df_subtraces_sub.subtraces)
                    # replace first entry with sub_hash of current building block
                    df_trace_txHash_rmBlocks.loc[df_trace_txHash_rmBlocks.trace_id_re == trace_id_reduced, "subaddresses_sub"] = \
                        "#" + dict_newBuildingBlock["sub_hash"]
                    # set subtraces to zero
                    df_trace_txHash_rmBlocks.loc[
                        df_trace_txHash_rmBlocks.trace_id_re == trace_id_reduced, "subtraces"] = 0
                    # drop (remove) all other subtraces from current buildingBlock -> shrink trace-tx copy
                    df_trace_txHash_rmBlocks.drop(
                        df_trace_txHash_rmBlocks[df_trace_txHash_rmBlocks.trace_id_re.isin(
                        df_subtraces_sub.loc[df_subtraces_sub.trace_id_re != trace_id_reduced, "trace_id_re"]
                        )].index.values.tolist(), inplace=True)

                    df_buildingBlocks = pd.concat([df_buildingBlocks,
                         pd.DataFrame({
                             'tx_hash': hasSubtrace.transaction_hash,
                             'to_protocol': hasSubtrace.to_protocol,
                             'MethodId': hasSubtrace.MethodId,
                             'MethodName': hasSubtrace.MethodName,
                             'isSubtrace': hasSubtrace.isIntern,
                             'ext_protocol': tr0.to_protocol,
                             'ext_address': tr0.to_address,
                             'ext_MethodId': tr0.MethodId,
                             'subtraces': dict_newBuildingBlock["subtraces"],
                             'subtraces_N': dict_newBuildingBlock["subtraces_N"],
                             'subaddresses': dict_newBuildingBlock["subaddresses"],
                             'subaddresses_sub': dict_newBuildingBlock["subaddresses_sub"],
                             'MethodIds': dict_newBuildingBlock["methodIds"],
                             'subtraces_hash' : dict_newBuildingBlock["subtraces_hash"],
                             'subaddresses_sub_hash': dict_newBuildingBlock["subaddresses_sub_hash"],
                             'sub_hash' : dict_newBuildingBlock["sub_hash"]
                         }, index = [sub_idx])
                    ])

                return (df_buildingBlocks.reset_index(drop=True))

            # if no subtraces exist
            else:

                # create a combined hash
                sub_hash = '0x' + str(hashlib.sha256(str(str(tr0.MethodId) + ',' +
                                                     "0" + ',' +
                                                     tr0.subaddresses_sub).encode('utf-8')).hexdigest())

                return (pd.DataFrame(data={
                        'tx_hash': tr0.transaction_hash,
                        'to_protocol': tr0.to_protocol,
                        'MethodId': tr0.MethodId,
                        'MethodName': tr0.MethodName,
                        'isSubtrace': False,
                        'ext_protocol': tr0.to_protocol,
                        'ext_address': tr0.to_address,
                        'ext_MethodId': tr0.MethodId,
                        'subtraces': "0",
                        'subtraces_N': 0,
                        'subaddresses': tr0.to_address,
                        'subaddresses_sub': tr0.subaddresses_sub,
                        'MethodIds': tr0.MethodId,
                        'subtraces_hash': "0",
                        'subaddresses_sub_hash': tr0.subaddresses_sub,
                        'sub_hash': sub_hash
                    }, index=[0]))


        tic = time.perf_counter();
        df_subtr = applyParallel(df_traces.groupby('transaction_hash'), processTrace);

        df_sub_gr = df_subtr.groupby(col_sub, dropna=False).count() \
            .reset_index().rename(columns={'tx_hash': 'count'}).sort_values("count", ascending=False)

        fn = os.path.join(path_preprocess_data, 'subTraces',
                          os.path.splitext(os.path.basename(csv_file))[0] + '_subTraces' +
                          os.path.splitext(os.path.basename(csv_file))[1])

        bf.df2csv(fn = fn, df = df_sub_gr)

        print(f"extracted subtraces from {csv_file} in {time.perf_counter() - tic}s")

    # get all sub_traces
    csv_files = bf.getFileWithExt(path=os.path.join(path_preprocess_data, 'subTraces'));
    csv_files.sort()

    # create all-in-one file
    fn = os.path.join(path_preprocess_data, 'subTraces', 'TxSubTraces_all.csv')
    df_all = pd.concat(list(map(lambda c: pd.read_csv(c,
                dtype={'tx_hash': str,
                    'to_protocol': str,
                    'MethodId': str,
                    'MethodName': str,
                    'isSubtrace': str,
                    'ext_protocol': str,
                    'ext_address': str,
                    'ext_MethodId': str,
                    'subtraces': str,
                    'subtraces_N': int,
                    'subaddresses': str,
                    'subaddresses_sub': str,
                    'MethodIds': str,
                    'subtraces_hash': str,
                    'subaddresses_sub_hash': str,
                    'sub_hash': str,
                    'count': int}), csv_files)), ignore_index=True) \
        .groupby(
        col_sub , dropna=False)['count'] \
        .sum() \
        .reset_index() \
        .sort_values("count", ascending=False)
    bf.df2csv(fn = fn, df = df_all)

    print("extracted subtraces: finished")



def main(args):
    try:
        # set limits if they don't exist
        n = 1500 if (args.number is None) else args.number # number of files
        l = 1e6 if (args.lines is None) else args.lines # number of lines

        bf.mkdir(os.path.join(path_preprocess_data))
        if (args.function in ['creaSig', 'create', 'all']):
            processMethodSignatures()
        if (args.function in ['creaPro', 'create', 'all']):
            createProtocolAddresses()
        if (args.function in ['split', 'all']): #, 'all'
            splitAWK()
        if (args.function in ['contracts', 'all']):
            extractContractCreations(file_numbers=n)
        if (args.function in ['extPro', 'create', 'all']):
            extandProtocolAddresses()
        if (args.function in ['filter', 'all']):
            filterFiles(file_numbers=n)
        if (args.function in ['agg', 'all']):
            aggFiles()
        if (args.function in ['ca_net', 'all']):
            get_CA_network()
        if (args.function in ['ca_net_pro', 'all']):
            get_CA_network_pro()
        if (args.function in ['subTr', 'all']):
            extractTxSubTraces(file_numbers=n)

    except:
        print("Error in executing")
        traceback.print_exc()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-f', '--function', help='Select function', type=str, required=False)
    parser.add_argument('-l', '--lines', help='Number of lines', type=int, required=False)
    parser.add_argument('-n', '--number', help='Limit the number of files', type=int, required=False)
    args = parser.parse_args()
    main(args)
