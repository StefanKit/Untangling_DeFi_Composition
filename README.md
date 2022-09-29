# Decentralized Finance (DeFi) Network

This repository contains the relevant files and scripts to get the results presented in the paper "Disentangling Decentralized Finance (DeFi) Compositions" by Kitzler et al. (2021).

It should contribute to answering the following overarching research question:

- **How are DeFi protocols intertwined?**

- **What are the most relevant building blocks of DeFi protocols?**

- **How would a hypothetical run on a stablecoin affect individual protocols?**


## Prerequisites

Make sure you have Python 3.9+ running.

	python3 --version

Create a virtual Conda environment from version 4.14.0+ with `environment.yml`, which contain R and Python packages.

    conda env create -f environment.yml

Activate the environment.

    conda activate comp_venv

Further, install Python libraries from pip, stored in `requirement.txt`.

    pip install -r requirements.txt
    
Note: in case of any issues, make sure you have all os-packages (e.g. pygraphviz, libxcursor-dev:i386) installed.

Scripts use directory references to access and store information. 
Copy the template and adjust the paths in the config file `config.yaml` to your reference directories.

	cp config.yaml.template config.yaml
	
## Run the Notebook

Add the conda environment to your ipython kernel to use it in your jupyter lab

    ipython kernel install --name=comp_venv --user
    
Run Jupyter Lab `jupyter-lab` and follow the `main.ipynb` Python notebook.

	jupyter-lab main.ipynb 

Make sure you select the right kernel ("Kernel" -> "Change Kernel" -> "comp_venv")

