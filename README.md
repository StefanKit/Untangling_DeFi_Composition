# DeFi_Compositions
Repo for "Disentangling Decentralized Finance (DeFi) Compositions"

This repository contains the relevant files and scripts to get the results presented in the DeFi ecosystem.

For the raw-files, only samples have been prepared in 
- ```./data/1_chain_data/traces/traces_0.csv``` and ```/traces_1.csv```: traces csv-files of etl-ethereum containing external and internal transactions
* ```./data/1_chain_data/contracts.csv```: existing CAs
* ```./data/1_chain_data/ethereum.functionsignatures.csv```: signature names from 4-byte
* ```./data/1_chain_data/trace_creations.csv```: only creation traces from etl-ethereum

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


### 1. Protocol Prepreparation and Building-Block extraction

All the steps of the data preparation and the building-block algorithm can be executed by the command
```sh
python ./scripts/1_preprocess.py -f "all"
```
The output files can be found in ```./data/1_preprocess_data```

#### 1.a Data Aqusation and Prepreparation

In the preprocessing some single steps needs to be executed in order to prepare the data and export the network.
Each of these steps can be assocated with a command after the flag "-f"

* "create": create unique ethereum signatures and a single protocol seed file
* "split": split the traces files into smaller chunks
* "contracts": add contracts from the traces_creation file, if they have not been added
* "extPro": extend the seed addresses with the traces_creation file
* "filter": filter the split traces by only external protocol calls
* "agg" : aggregate identical transactions and count them
* "ca_net" : extract data for the DeFi CA network
* "ca_net_pro" : extract data for the DeFi Pro network


Here is an example to call one of the functions:
```sh
python ./scripts/1_preprocess.py -f "create"
```
#### 1.b Building Block Extraction

* "subTr": in this step the building blocks of subtraces will be extracted


### 2. Protocol Network Construction

The purpose of the file 2_network_analysis.py is to have a script that performs network metrics
on different dataframes. Two parameters are required in order to execute it:

-d --dataframe: provide a string corresponding to a specific dataframe

-s --select: option to select which section to analyse

Currently, the options are: 

* dataframe: 
    * "defi_ca_network" = DeFi CA network
    * "defi_ca_network_pro" = Protocol network 
  
* select: 
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

The analysis of the degree distribution is completed by running the script powerlaw.R, which takes both netowkr files of the section 2 script as an input. 
It can be run as follows:

```sh
Rscript ./scripts/powerlaw.R
```
