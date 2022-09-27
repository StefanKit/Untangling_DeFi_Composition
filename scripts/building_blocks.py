import scripts.basicFun as bf
import os as os
import pandas as pd
import glob
import hashlib
import time
from multiprocessing import Pool, cpu_count
from joblib import Parallel, delayed

path_building_block_data = bf.__path_building_block_data__

__col_sub__ = ['to_protocol', 'isSubtrace','subtraces_N', 'MethodId', 'MethodName',
               'subtraces', 'addresses', 'addresses_sub','MethodIds',
               'subtraces_hash', 'addresses_sub_hash', 'sub_hash',
               'ext_protocol', 'ext_address','ext_MethodId']

def applyParallel(dfGrouped, func):
    retLst = Parallel(n_jobs=cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)

def extractTxBuildingBlocks(file_numbers, block_range):

    # erc20 method ids 
    mId_ERC20 = list(  {'totalSupply':"0x18160ddd", 'balanceOf':'0x70a08231', 'transfer':'0xa9059cbb',
                         'transferFrom':'0x23b872dd', 'approve':'0x095ea7b3', 'allowance':'0xdd62ed3e'}.values())

    cas = pd.read_csv(os.path.join(bf.__path_preprocess_data__, 'contract_creations', "contracts.csv.gz"), dtype={'isERC20': str})

    trace_fil_files = glob.glob(os.path.join(os.path.abspath(bf.__path_preprocess_data__), "filtered_traces") + "/**/*.csv.gz",
                                recursive=True)
    trace_fil_files.sort()

    # filter for trace files in reference block-range
    trace_fil_files = list(filter(lambda f: bf.range_intersect(bf.fn2blockrange(f), block_range), trace_fil_files))


    if (file_numbers < len(trace_fil_files)):
        trace_fil_files = trace_fil_files[:file_numbers]

    # for each file 
    for trace_fil_file in trace_fil_files:

        bf.mkdir(path = os.path.join(bf.__path_building_block_data__,'building_blocks'))

        # read traces into data frame
        df_traces = pd.read_csv(trace_fil_file, dtype={'value': 'float'}, compression='gzip')\
            .drop("Unnamed: 0", axis = 1)

        # filter subset of by block range
        df_traces = df_traces.loc[(df_traces.block_number >= min(block_range)) &
                                  (df_traces.block_number <= max(block_range))]
        df_traces = df_traces.sort_values('trace_id')

        # add erc20 compatibility to identify assets
        df_traces = pd.merge(left=df_traces, right=cas[['address', 'isERC20']].rename(columns={'address': 'to_address'}),
                          how='left', on="to_address")

        # define process to extract building blocks for each trace with same tx-hash
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
            # copy col df_building_blocks.to_address
            df_trace_txHash.loc[:, "addresses_sub"] = df_trace_txHash.loc[:, "to_address"]
            # substitute deployed addresses with DEPLOYED and enumerate
            df_trace_txHash.loc[df_trace_txHash.isDeployed == True, "addresses_sub"] = \
                df_trace_txHash.loc[df_trace_txHash.isDeployed == True, "to_protocol"].apply(lambda s: s + "-DEPLOYED")
            # substitute deployed addresses with DEPLOYED and enumerate
            df_trace_txHash.loc[df_trace_txHash.isAsset == True, "addresses_sub"] = "ASSET"
            # create address list in order of execution

            # store external call only
            tr0 = df_trace_txHash[df_trace_txHash.trace_address.isna()].iloc[0]

            # extract all entries that start to a known protocol and have building blocks
            df_hasBuildingBlocks = df_trace_txHash.loc[(~(df_trace_txHash.to_protocol.isna())) &
                                                   (df_trace_txHash.subtraces > 0) &
                                                   (df_trace_txHash.isAsset == False), :].copy()
            # df_hasBuildingBlocks = df_trace_txHash.loc[(~(df_trace_txHash.to_protocol.isna())) , :].copy()

            # define an empty building block list
            l_buildingBlocks = []

            # if building blocks exist
            if (len(df_hasBuildingBlocks) > 0):

                # label all intern building blocks which appear within the trace
                df_hasBuildingBlocks.loc[:, "isIntern"] = (~(df_hasBuildingBlocks.loc[:, "trace_address"].isna()))

                # define empty df to collect building blocks of transaction_hash
                l_buildingBlocks = []
                df_buildingBlocks = pd.DataFrame()

                # create additional tx-traces, which where building blocks will be removed
                df_trace_txHash_rmBlocks = df_trace_txHash.copy()

                # for loop df_hasBuildingBlocks - bottom up: starting with the last entries
                df_hasBuildingBlocks.sort_values("trace_id_re", ascending=False, inplace=True) # sort by starting with the last
                df_hasBuildingBlocks.reset_index(drop=True, inplace=True)

                for sub_idx, hasSubtrace in df_hasBuildingBlocks.iterrows():

                    # store components to create a building-block
                    dict_newBuildingBlock = dict()

                    trace_id_reduced = hasSubtrace.trace_id_re
                    df_building_blocks = df_trace_txHash[df_trace_txHash.trace_id_re \
                        .apply(lambda s: (str(s).find(
                        trace_id_reduced) >= 0))].copy()  # find matching trace_id's and extract their subtraces
                    df_building_blocks.sort_values("trace_id_re", ascending=True, inplace=True)
                    df_building_blocks.reset_index(inplace=True)

                    def getStringOfSeries(series):
                        return(str('_').join(list(map(str, series.to_list()))))

                    ## extract subtrace-block content and identifiers from subtraces
                    # create list of number of edges
                    subtraces = df_building_blocks.subtraces # represent graph-tree structure
                    dict_newBuildingBlock["subtraces_N"] = sum(subtraces) # get number of links/calls
                    dict_newBuildingBlock["subtraces"] = getStringOfSeries(df_building_blocks.subtraces) # subtraces identifier
                    # create address list in order of execution
                    dict_newBuildingBlock["addresses"] = getStringOfSeries(df_building_blocks.to_address) # address list identifier
                    # create methodId list
                    dict_newBuildingBlock["methodIds"] = getStringOfSeries(df_building_blocks.MethodId) # method list identifier
                    # create substituted (abstracted) subaddress list
                    dict_newBuildingBlock["addresses_sub"] = getStringOfSeries(df_building_blocks.addresses_sub)

                    # create a combined hash
                    dict_newBuildingBlock["sub_hash"] = '0x' + str(hashlib.sha256(str(str(dict_newBuildingBlock["methodIds"]) + ',' +
                                                             dict_newBuildingBlock["subtraces"] + ',' +
                                                             dict_newBuildingBlock["addresses_sub"]).encode('utf-8')).hexdigest())

                    ## remove just current building-block from trace-tx copy
                    # find all subtraces in shrinking trace-tx copy
                    df_subtraces_sub = df_trace_txHash_rmBlocks[df_trace_txHash_rmBlocks.trace_id_re \
                        .apply(lambda s: (str(s).find(trace_id_reduced) >= 0))]
                    # create hash-list of addresses_sub and subtraces
                    dict_newBuildingBlock["addresses_sub_hash"] = getStringOfSeries(df_subtraces_sub.addresses_sub)
                    dict_newBuildingBlock["subtraces_hash"] = getStringOfSeries(df_subtraces_sub.subtraces)
                    # replace first entry with sub_hash of current building block
                    df_trace_txHash_rmBlocks.loc[df_trace_txHash_rmBlocks.trace_id_re == trace_id_reduced, "addresses_sub"] = \
                        "#" + dict_newBuildingBlock["sub_hash"]
                    # set subtraces to zero
                    df_trace_txHash_rmBlocks.loc[
                        df_trace_txHash_rmBlocks.trace_id_re == trace_id_reduced, "subtraces"] = 0
                    # drop (remove) all other subtraces from current buildingBlock -> shrink trace-tx copy
                    df_trace_txHash_rmBlocks.drop(
                        df_trace_txHash_rmBlocks[df_trace_txHash_rmBlocks.trace_id_re.isin(
                        df_subtraces_sub.loc[df_subtraces_sub.trace_id_re != trace_id_reduced, "trace_id_re"]
                        )].index.values.tolist(), inplace=True)


                    l_buildingBlocks.append(pd.DataFrame({
                             'tx_hash': hasSubtrace.transaction_hash,
                             'block_number': tr0.block_number,
                             'to_protocol': hasSubtrace.to_protocol,
                             'MethodId': hasSubtrace.MethodId,
                             'MethodName': hasSubtrace.MethodName,
                             'isSubtrace': hasSubtrace.isIntern,
                             'ext_protocol': tr0.to_protocol,
                             'ext_address': tr0.to_address,
                             'ext_MethodId': tr0.MethodId,
                             'subtraces': dict_newBuildingBlock["subtraces"],
                             'subtraces_N': dict_newBuildingBlock["subtraces_N"],
                             'addresses': dict_newBuildingBlock["addresses"],
                             'addresses_sub': dict_newBuildingBlock["addresses_sub"],
                             'MethodIds': dict_newBuildingBlock["methodIds"],
                             'subtraces_hash' : dict_newBuildingBlock["subtraces_hash"],
                             'addresses_sub_hash': dict_newBuildingBlock["addresses_sub_hash"],
                             'sub_hash' : dict_newBuildingBlock["sub_hash"]
                         }, index = [sub_idx]))

                #return (df_buildingBlocks.reset_index(drop=True))

            # if no subtraces exist
            else:

                # create a combined hash
                sub_hash = '0x' + str(hashlib.sha256(str(str(tr0.MethodId) + ',' +
                                                     "0" + ',' +
                                                     tr0.addresses_sub).encode('utf-8')).hexdigest())

                l_buildingBlocks.append(pd.DataFrame(data={
                        'tx_hash': tr0.transaction_hash,
                        'block_number': tr0.block_number,
                        'to_protocol': tr0.to_protocol,
                        'MethodId': tr0.MethodId,
                        'MethodName': tr0.MethodName,
                        'isSubtrace': False,
                        'ext_protocol': tr0.to_protocol,
                        'ext_address': tr0.to_address,
                        'ext_MethodId': tr0.MethodId,
                        'subtraces': "0",
                        'subtraces_N': 0,
                        'addresses': tr0.to_address,
                        'addresses_sub': tr0.addresses_sub,
                        'MethodIds': tr0.MethodId,
                        'subtraces_hash': "0",
                        'addresses_sub_hash': tr0.addresses_sub,
                        'sub_hash': sub_hash
                    }, index=[0]))

            return(pd.concat(l_buildingBlocks, ignore_index=True)) #, ignore_index=True).reset_index(drop = True)


        tic = time.perf_counter();
        df_subtr = applyParallel(df_traces.groupby('transaction_hash'), processTrace).reset_index(drop = True);

        #df_sub_gr = df_subtr.groupby(__col_sub__, dropna=False).count() \
        #    .reset_index().rename(columns={'tx_hash': 'count'}).sort_values("count", ascending=False)

        fn_old = str(os.path.basename(trace_fil_file)).split(".",1)
        fn = os.path.join(bf.__path_building_block_data__, 'building_blocks',
                          fn_old[0] + '_building_blocks.' + fn_old[1])

        bf.df2csvgz(fn = fn, df = df_subtr)

        print(f"extracted subtraces from {trace_fil_file} in {time.perf_counter() - tic}s")

    print("extracted subtraces: finished")

def aggTxSubTraces(file_numbers, block_range):

    bb_files = glob.glob(os.path.join(os.path.abspath(bf.__path_building_block_data__), "building_blocks") + "/**/*.csv.gz",
                                recursive=True)
    bb_files.sort()

    # filter for trace files in reference block-range
    bb_files = list(filter(lambda f: bf.range_intersect(bf.fn2blockrange(f), block_range), bb_files))

    if (file_numbers < len(bb_files)):
        bb_files = bb_files[:file_numbers]

    # create all-in-one file
    bf.mkdir(path=os.path.join(bf.__path_building_block_data__, 'building_blocks_agg'))
    fn = os.path.join(bf.__path_building_block_data__, 'building_blocks_agg', f'BB_agg_'
                                                         f'{str(int(block_range[0]))}-'
                                                         f'{str(int(block_range[1]))}_all.csv.gz')

    def read_and_preprocess(filepath):
        df_raw = pd.read_csv(filepath, dtype=str,compression="gzip")

        df_p1 = df_raw.drop("Unnamed: 0", axis=1).rename(columns = {
            'subaddresses': 'addresses',
            'subaddresses_sub': 'addresses_sub',
            'subaddresses_sub_hash':'addresses_sub_hash'
        })

        # define type str
        for c in ['tx_hash', 'to_protocol', 'MethodId', 'MethodName', 'isSubtrace',
                  'ext_protocol', 'ext_address', 'ext_MethodId',
                  'subtraces','addresses','addresses_sub','MethodIds','subtraces_hash','addresses_sub_hash','sub_hash']:
            df_p1[c] = df_p1[c].astype(str)

        # define type int
        for i in ['subtraces_N','block_number']:
            df_p1[i] = df_p1[i].astype(int)

        # filter block range within the trace dataframe
        df_p1 = df_p1.loc[(df_p1.block_number >= min(block_range)) &
                          (df_p1.block_number <= max(block_range))]

        df_g = df_p1.groupby(__col_sub__, dropna=False).count() \
            .reset_index().rename(columns={'tx_hash': 'count'}).sort_values("count", ascending=False)

        return(df_g)

    if(len(bb_files)>0):
        df_all = pd.concat(list(map(read_and_preprocess, bb_files)), ignore_index=True) \
            .groupby(
            __col_sub__ , dropna=False)['count'] \
            .sum() \
            .reset_index() \
            .sort_values("count", ascending=False) \
            .reset_index(drop = True)
        bf.df2csvgz(fn = fn, df = df_all)

        print("aggregate subtraces: finished")
    else:
        print("aggregate subtraces: failed")