import scripts.basicFun as bf
import pandas as pd
import glob
import os
import numpy as np
import matplotlib
# matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

path_building_block_data = bf.__path_building_block_data__

def case_study(addr_token = str("0xdAC17F958D2ee523a2206206994597C13D831ec7").lower(),  # token address (usd tether)
               file_relpath_bb_agg_info = "/building_blocks_agg/BB_agg_11565019-12964999_all.csv.gz",
               fn_token_dep = "0xdAC1-11565019-12964999_Protocol_Depenency.csv.gz"):

    file_path_bb_agg_info = path_building_block_data + file_relpath_bb_agg_info
    print(file_path_bb_agg_info)
    # read building blocks
    df_bb = pd.read_csv(file_path_bb_agg_info, low_memory=False).drop("Unnamed: 0", axis=1)
    df_bb = df_bb.loc[df_bb.subtraces != '0'].copy()  # remove empty external blocks

    # bb that contain the token addr (but maybe only indirectly in sub-bb)
    df_bb.loc[:,"addr_token_contained"] = df_bb.addresses.str.contains(addr_token) # token addr is contained in bb, but possibly in another sub-bb
    idx = df_bb.loc[df_bb.addr_token_contained].index # extract index of those contained blocks

    # extract the directly contained address in BB and verify if they include the token address
    df_bb_lookup = df_bb.loc[:,["sub_hash","addresses_sub_hash"]].drop_duplicates().set_index("sub_hash").copy() # look-up
    # create lookup: for each bb-hash - the contained address + substituted addresses (e.g. ASSET or Sub-BB-Hash)
    def directAddr(entry):
        # extract
        ash = entry.addresses_sub_hash
        a = entry.addresses #

        mask = ash.split("_") # split into single address / substituded-entries
        for i, e in enumerate(mask):
            if (not (str(e).startswith("#"))):
                mask[i] = "*"
        mask = "_".join(mask)

        while (str(mask).find("#") >= 0):
            v = str(mask).split("_")
            for i, e in enumerate(v):
                if (str(e).startswith("#")):
                    v[i] = df_bb_lookup.loc[str(e).strip("#")].values[0]
            mask = "_".join(v)

        return ("_".join(np.array(str(a).split("_"))[np.array(str(mask).split("_")) == "*"].tolist()))
    df_bb.loc[idx, "addresses_direct"] = df_bb.loc[idx].apply(directAddr,axis = 1) # extract the directly contained addresses
    df_bb.loc[:,"addresses_direct"] = df_bb.addresses_direct.str.contains(addr_token) # is directly included?
    del df_bb_lookup
    df_bb.loc[df_bb.addresses_direct.isna(),"addresses_direct"] = False # not included -> also not directly included
    df_bb.addresses_direct = df_bb.addresses_direct.astype(bool) # change type to bool

    df_bb.loc[:,"token"] = (df_bb['addr_token_contained']  + df_bb['addresses_direct'] * 1).astype(int) # create single field
    # -> {0:'Token_false',1:'Token_indirect', 2:'Token_direct'}

    # agg by groupby selected columns
    col_gr = ['to_protocol', 'MethodId', 'MethodName', 'isSubtrace', 'subtraces_N', 'subtraces', 'subtraces_hash', 'addresses_sub', 'addresses_sub_hash', 'sub_hash']
    # only take indirectly called BB into account + isSubtrace
    df_bb_gr = df_bb.groupby(col_gr+['token','ext_protocol'])['count'].sum().reset_index().reindex()
    del df_bb

    ## EVALUATE on PROTOCOL LEVEL
    df_bb_gr_split_pro = df_bb_gr.pivot_table(index=col_gr+['ext_protocol'],
                                          columns='token', values='count',aggfunc=np.sum, fill_value=0).\
        reset_index().\
        rename_axis(None, axis=1).rename({0:'TOKEN_false',1:'TOKEN_indirect', 2:'TOKEN_direct'},axis = 1)

    df_usdt_pro_ext_stat = df_bb_gr_split_pro.groupby(['to_protocol','ext_protocol'])['TOKEN_direct','TOKEN_indirect','TOKEN_false'].sum().reset_index()
    df_usdt_pro_ext_stat.loc[:,'count'] = df_usdt_pro_ext_stat.loc[:,['TOKEN_direct','TOKEN_indirect','TOKEN_false']].sum(axis = 1)

    df_pro_stat = df_usdt_pro_ext_stat.groupby('to_protocol')[['TOKEN_direct','TOKEN_indirect','count']].sum().reset_index()
    df_pro_stat.loc[:,"TOKEN_perDir"] = df_pro_stat.loc[:,"TOKEN_direct"] / df_pro_stat.loc[:,"count"]
    df_pro_stat.loc[:,"TOKEN_perIn"] = df_pro_stat.loc[:,"TOKEN_indirect"] / df_pro_stat.loc[:,"count"]


    bf.mkdir(path=os.path.join(path_building_block_data, 'case_study'))
    path_token_dep = path_building_block_data + '/case_study/'+fn_token_dep
    print("Write: "+path_token_dep)
    bf.df2csv(df_pro_stat.set_index('to_protocol').loc[:,["TOKEN_perDir","TOKEN_perIn"]],path_token_dep) #.plot.bar()

    return(df_pro_stat.set_index('to_protocol').loc[:,["TOKEN_perDir","TOKEN_perIn"]])

