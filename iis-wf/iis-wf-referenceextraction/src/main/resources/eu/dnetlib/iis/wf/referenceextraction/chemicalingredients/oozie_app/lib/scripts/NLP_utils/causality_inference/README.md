# Causality Inference
---
## Introduction
Given a corpus of texts, that consists of abstracts and titles from papers and has to do with chemical compounds that are related to the food and cosmetics industry, determine if the compound has a positive or negative relation to several adverse effects (cancer, neurotoxicity etc). For this task, a model was built that can correlate the provided information with the potential hazard or the absence thereof. This amounts to determining whether a compound has a positive to notions like cancer, irritation, allergies and toxicity. In order to do this a model was trained that can infer whether a sentence bears causal meaning. Additionally to the causality inference of the sentence the compound names and the adverse effects must be present in the same sentence.

## Methodology
To solve the problem of inferring the compound toxicity, the problem is transformed into a classification one. Namely, to identify whether a sentence has a meaning that can be described as “Compound A causes B”. If a given compound fulfils multiple times the criteria: both the name of a compound and the name of an adverse effect exist in a sentence and the sentence is classified as causal, then the chance that the compound is related causally with the adverse effect increases.

## Installation
### Conda environment
`curl -sL "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" > "Miniconda3.sh"`
* Restart your Terminal. Now your prompt should list which environment is active (in this case “base”, i.e. the default).
*    Update Conda using the command:
`conda update conda`
*    After installation, delete the installer:
`rm Miniconda3.sh`
*    Install the program “wget” using Conda to download any files using CLI:
`conda install wget`
*    Create a `conda` environment named Phase2 and install python v3.6.9
`conda create -n Phase3 python=3.7.9`
*    Activate conda environment 
`conda activate Phase3`
*    Clone github repo:
`git clone https://github.com/ingredio/Phase3/entity_extraction`
`cd entity`
*    Install dependencies:
`pip3 install -r requirements.txt`
* Download bert model and vocabulary files from this [link](https://archive.org/details/CausalySmall) and save them to input/bert_based_uncased

### Python requirements
* `pandas==1.1.5`
* `transformers==3.0.0`
* `numpy==1.19.5`
* `torch==1.7.0`
* `tensorflow==2.4.1`
* `nltk==3.2.5`
* `tqdm==4.41.1`
* `scikit_learn==0.24.1`


## Datasets
For this goal, the dataset must include two columns. A column named “Sentence” and a second column called “Annotated_causal”. The whole dataset can be found here. A small sample of this dataset is shown below:

Sentence|Annotated_causal
-|-
Whatever the mechanism , active smoking is an important modifiable factor that seems to be associated with a poor response to MTX .|1
Data were collected on the incidence of POAF lasting more than 5 minutes and secondary outcomes , including the length of hospitalization , guideline adherence rate , adverse events , and timeliness of POAF treatment .|0

## Usage
* Step 1 - Constants setup (`config.py`)

This file contains constant values that are used throughout the process, like paths, number of epochs, batch size etc. 

* Step 2 - Data handling, Training & Evaluation

`python train.py`

This script loads the training sets, splits it to train and validation sets and converts them to pytorch tensors. Afterwards, it loads the BERT model, its parameters and the optimizer. Finally, it trains the model for the number of epochs set in the config file and saves the best model based on the accuracy.


* Step 3 - Causality Inference

`python predict.py`

The `predict.py` file loads the best model found during training. Then it imports a file that contains the json entries classified during the second phase and uses the BERT tokenizer to transform the sentence into numerical representation.Finally, it uses the best model to predict the causality inference of each sentence.
