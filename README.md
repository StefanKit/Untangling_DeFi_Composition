# DeFi_Compositions
This repository provides the tools to reproduce the results shown in the paper ["Disentangling Decentralized Finance (DeFi) 
Compositions"](https://depositonce.tu-berlin.de/bitstream/11303/13867/4/Kitzler_etal_Disentangling_2021_Cover.pdf) by 
Kitzler et al. (2021), the first study on compositions of Decentralized Finance (DeFi) protocols. It contains the scripts and a
sample of the data (as the full dataset is too large), as well as the sources where to find the full data we used.

The samples of the on-chain data are stored in the ```./data/1_chain_data/``` folder:
* ```./data/1_chain_data/traces/traces_0.csv``` and ```/traces_1.csv```: csv files of traces, gathered through an OpenEthereum
client and ethereum-etl (https://github.com/blockchain-etl/ethereum-etl), that contain both external and internal transactions.
In our analyses, transactions go from 01-Jan-2021 (block 11,565,019) to 05-Aug-2021 (block 12,964,999). 
Note that there are two different files: our code takes all traces.csv files stored in the ```./data/1_chain_data/traces/``` folder.
In this way, the dataset can be extended by simply adding new traces.csv files for more recent time frames.
* ```./data/1_chain_data/ethereum.functionsignatures.csv```: file that maps the called method of a code account into human-readable
function signatures. This dataset was created by scanning the 4Byte lookup service (https://www.4byte.directory/).
* ```./data/1_chain_data/trace_creations.csv```: list of code account creation transactions. The full sample was obtained
by parsing the Ethereum transactions from the first CA created until the end of our observation period. 
* ```./data/1_chain_data/contracts.csv```: file containing reduced information extracted from the ```trace_creations.csv``` file 
for faster access to existing smart contracts. It contains all adresses of all code accounts and removes failed creations.

In the folder ```./data/1_seed_data/protocols/``` we provide manually curated ground-truth lists of protocol-specific addresses 
based on off-chain public sources of information, such as protocol websites and related documentations. In total, we 
gathered 1407 addresses for the 23 DeFi protocols that we identified as the most relevant ones in the observation period. 

## Prerequisites

Make sure you have Python 3.6+ running.

	python3 --version

Create a virtual Conda environment and install all required dependencies

    conda create -n conda_venv 
    conda activate conda_venv
    conda update --all
    conda install -c conda-forge graph-tool
    pip install -r requirements.txt
    
Some scripts generate figures and tables. Define the output path by generating a custom config file:

	cp config.yaml.template config.yaml
  
Also make sure that you have R 3.6.3+ installed


### 1. Protocol preparation and Building-Block extraction

All the steps of the data preparation and the building-block algorithm can be executed by the command
```sh
python ./scripts/1_preprocess.py -f "all"
```
The output files can be found in ```./data/1_preprocess_data```

Alternatively, the preprocessing and the building block extraction can also be executed in single steps, each of them 
being associated with one of the following commands:

* "create": create unique ethereum signatures and a single protocol seed file
* "split": split the traces files into smaller chunks
* "contracts": add contracts from the traces_creation file, if they have not been added
* "extPro": extend the seed addresses with the traces_creation file
* "filter": filter the split traces by only external protocol calls
* "agg" : aggregate identical transactions and count them
* "ca_net" : extract data for the DeFi CA network
* "ca_net_pro" : extract data for the DeFi Pro network
* "subTr": in this step the building blocks of subtraces will be extracted

Again, the command must be added after the flag "-f". 
Here is an example on how to call one of the functions:

```sh
python ./scripts/1_preprocess.py -f "create"
```

### 2. Protocol Network Construction
The purpose of the file ```./scripts/2_network_analysis.py``` is to have a script that performs network metrics on different dataframes. 
Two parameters are required in order to execute it, that is, "-d" (--dataframe) and "-s" (--select). The former is a 
parameter that requires a string corresponding to the specific dataframe to conduct the analyses on, while the latter is 
an option to select which metrics to compute.

Currently, the options for "-d" are: 

* "defi_ca_network" = DeFi CA network
* "defi_ca_network_pro" = Protocol network 
  
and for "-s":

* "all" = compute all steps
* "summary" = summary stats on the network
* "powerlaw" = provides data for to execute the power law analysis with a script which is described below
* "components" = analysis of the network components
* "communities" = community detection algorithm analysis

As an example, the script can be called as follows: 

```sh
python ./scripts/2_network_analysis.py -d "defi_ca_network_pro" -s "all"
python ./scripts/2_network_analysis.py -d "defi_ca_network" -s "all"
```

The output of the analyses can be found in the output directory.



### 3. Further Data Analysis and Plots

#### Flatten

A way to visualize and flatten the nested building-block structure is by running this script:

```sh
Rscript ./scripts/3_traces_plot.R 
```
#### Powerlaw

The analysis of the degree distribution is completed by running the script powerlaw.R, which takes both network
files of the section 2 script as an input. It can be run as follows:

```sh
Rscript ./scripts/powerlaw.R
```
