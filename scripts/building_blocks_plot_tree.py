import scripts.basicFun as bf
import pandas as pd
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_pydot import graphviz_layout
import os
import seaborn as sns
import numpy as np

path_building_block_data = bf.__path_building_block_data__


def singleTraceGraph(df_fil_traces, tx_hash = "0x83e9bb89d39ba11e15f173d167269c2173f5c1ea4c410ddeff1caf3916d91993"):

    trace_1 = df_fil_traces.loc[df_fil_traces.transaction_hash == str(tx_hash).lower()].copy()
    # 0x83e9bb89d39ba11e15f173d167269c2173f5c1ea4c410ddeff1caf3916d91993
    # #0xe00accab406fc1473e3ff4c839615d05d94b6d251e1348726ce2096755db6871

    trace_1.loc[trace_1.trace_address.isna(),"from_address_"] = trace_1.loc[trace_1.trace_address.isna(),"from_address"]
    trace_1.loc[~trace_1.trace_address.isna(),"from_address_"] = trace_1.loc[~trace_1.trace_address.isna()].apply(lambda x: x.from_address+"_"+"|".join(str(x.trace_address).split("|")[:-1]),axis = 1)
    trace_1.loc[~trace_1.trace_address.isna(),"to_address_"] = trace_1.loc[~trace_1.trace_address.isna(),"to_address"] + "_" + trace_1.loc[~trace_1.trace_address.isna(),"trace_address"]
    trace_1.loc[trace_1.trace_address.isna(),"to_address_"] = trace_1.loc[trace_1.trace_address.isna(),"to_address"] + "_"

    g1 = nx.from_pandas_edgelist(trace_1, "from_address_","to_address_",create_using = nx.DiGraph)
    pos = graphviz_layout(g1, prog="dot")

    # nx.draw(g1,pos)
    return(g1)


def create_pro_tree_plot(select_protocol = "aave",
                         file_relpath_pro_tree = "/building_blocks_agg/pro_tree_14000000-14000999.csv.gz"):

    path_pro_tree = path_building_block_data + file_relpath_pro_tree
    # read bb protocol tree
    df_tree = pd.read_csv(path_pro_tree, sep = ";", decimal=",", dtype={'values':float}).drop("Unnamed: 0", axis = 1)


    # copy bb tree for selected protocol
    df_tree_pro_mod = df_tree.loc[df_tree.ext_protocol == select_protocol].reset_index(drop = True).copy()

    def rmBranches(id):
        v = str(id).split("_")
        return ("_".join(list(filter(lambda s: str(s).count(",") == 0, v))))

    # modify 'ids' and 'parents' columns by removing the branch (middle layer) entries
    df_tree_pro_mod.loc[:,"ids"] = df_tree_pro_mod.ids.apply(rmBranches)
    df_tree_pro_mod.loc[:,"parents"] = df_tree_pro_mod.parents.apply(rmBranches)

    # re-agg them
    df_tree_pro_mod = df_tree_pro_mod.groupby(['labels','ids','parents']).values.sum().reset_index()

    g2 = nx.from_pandas_edgelist(df_tree_pro_mod, source = "parents", target = "ids", edge_attr="values",
                             create_using=nx.DiGraph())

    # node labels
    nx.set_node_attributes(g2, df_tree_pro_mod.set_index("ids").loc[:,"labels"].to_dict(), name="labels")
    nx.set_node_attributes(g2, {'nan':"EOA"}, name="labels")
    labels = nx.get_node_attributes(g2,'labels')
    # reduce labels by only using the first 2 layers
    for i in list(filter(lambda s: len(str(s).split("_"))>1, list(labels))):
        labels.pop(i)

    # node colors
    df_colors_node = pd.DataFrame({'labels':df_tree_pro_mod.labels.unique()})
    df_colors_node["color_hex"] = sns.color_palette("tab20",len(df_colors_node)).as_hex()
    df_colors_node["color_rgba"] = df_colors_node["color_hex"].apply(matplotlib.colors.to_rgba)
    df_tree_0x_mod = pd.merge(df_tree_pro_mod,df_colors_node,how="left",on="labels")
    nx.set_node_attributes(g2, df_tree_0x_mod.set_index("ids").loc[:,"color_rgba"].to_dict(), name="color")
    nx.set_node_attributes(g2, {"nan":matplotlib.colors.to_rgba('#000000',1)}, name="color")
    color_node = list(nx.get_node_attributes(g2, 'color').values())

    edge_attr = nx.get_edge_attributes(g2,name='values')

    pos = graphviz_layout(g2, prog="dot") #twopi, "dot"

    fig, ax = plt.subplots(1, figsize=(10,5))

    nx.draw(g2, pos,
            node_color=color_node,
            edge_cmap=plt.cm.copper_r,
            node_size = 40,
            alpha = 0.8,
            width = 0.5,#list(map(np.sqrt,list(edge_attr.values()))),#0.5,
            #edge_color = list(map(np.log,list(edge_attr.values()))),#0.5,
            vmin=0, vmax=100,
            ax = ax,
            labels = labels, #labels={'nan':"EOA"}
            with_labels=True,
            verticalalignment='bottom',
            horizontalalignment='right') #,

    # create legend
    for index, row in df_colors_node.iterrows():
        ax.scatter([],[], c=[row['color_rgba']] , label=r'$\mathdefault{'+row['labels']+'}$') #+x['labels'] #x['color_rgba']

    ax.legend(scatterpoints=1, bbox_to_anchor=(1,1), borderaxespad=1,title="Building Blocks \nof DeFi protocols", fontsize='large', title_fontsize = 'large')

    plt.tight_layout()
    # fig.savefig("./outfiles/figures/bb_tree_aave.pdf")


