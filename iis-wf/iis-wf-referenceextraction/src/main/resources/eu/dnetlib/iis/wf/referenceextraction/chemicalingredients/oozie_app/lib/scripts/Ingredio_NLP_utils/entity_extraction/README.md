# Named Entity Recognition
---
## Introduction

In this stage, a model is constructed that takes as input a positively classified paper from the OpenAIRE data as relevant and outputs the best candidate words that represent chemical compounds. To implement this task, Bidirectional Encoder Representations from Transformers (BERT) is used. To train the BERT model, a set of peer-reviewed articles was created which contains chemical ingredients and annotated with part of speech tags.

## Methodology

In order for BERT to be able to extract chemical compound names from sentences a specific training set must be created. The train set consists of tokenized words taken from abstracts, their part-of-speech tag (POS tag), index of sentence and a label that shows if each word is a compound or not. These abstracts were a part of the training set used in the second phase. The training set is imported into the script, which applies preprocessing and transforms it to pytorch tensors for BERT to be able to train and validate on it. After training BERT, the script saves the model that performs the best  and uses it in order to extract compound names from the papers that were classified in phase 2.

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
*    Enter the `entity_extraction` folder: `cd entity entity_extraction`
*    Install dependencies:
`pip3 install -r requirements.txt`

### Python requirements
* `pandas==1.1.5`
* `transformers==3.0.0`
* `numpy==1.19.5`
* `torch==1.7.0`
* `tensorflow==2.4.1`
* `nltk==3.2.5`
* `tqdm==4.41.1`
* `scikit_learn==0.24.1`

## Dataset

As a dataset for this task a CSV file is created where each line contains a word, its part of speech tag, PMID of the paper the word was found in, the index of sentence and a label that shows if each word is a compound or not. The dataset used can be downloaded from [here](https://drive.google.com/drive/folders/1hlnRv9GIaQSVUQeSBAkc-k8BJIDWwls7) and should exist in the root folder.
A sample of the used training set can be seen below:

PMID|Word|POS|Sentence|Tag
-|-|-|-|-
0|18560757|And|CC|5322|0
1|18560757|adenosine|NN|5322|1
2|18560757|To|TO|5322|0

## Usage

* Step 1 - Constants setup (`config_task1.py`)

This file contains constant values that are used throughout the process, like paths, number of epochs, batch size etc. 

* Step 2 - Data handling, Training & Evaluation

`python train_task1.py`

This script imports the training set, groups the words into sentences and applies preprocessing (BERT tokenizer, add padding or cut sentences based on the MAX_LEN value, split to train & test sets and transform to pytorch tensors). Then for each epoch it trains and evaluates BERT on those tensors in batches. Finally, it saves the best model based on the precision.


* Step 3 - Named entity recognition

`python predict_task1.py`

Predict_task1.py loads the best model found during training. Then it imports a file that contains the json entries classified during the second phase. This file should exist in the root folder and its name should be defined in the config_task1.py file. The script then applies preprocessing to it where it splits the abstract into sentences, uses BERT tokenizer to tokenize the sentences and transforms them to torch tensors. It then adds a label (1 if the word is classified as a compound name and 0 if not) to each word based on the prediction. Finally it returns the words classified as compound names.