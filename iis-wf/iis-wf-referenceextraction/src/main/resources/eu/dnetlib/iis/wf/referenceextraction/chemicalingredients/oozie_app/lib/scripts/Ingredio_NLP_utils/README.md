# Natural Language Process Utilities
---
## NLP projects related to biomedical text.

* Clone github repo: `git clone https://github.com/ingredio/Phase3`

Installation instructions for each tool, are provided in their respective folders.

### Data Mining
* Model that can determine whether a scientific text is correlated with chemical substances found in food and cosmetics that exhibit carcinogenicity, toxicity, irritation and allergies. The model is trained on biomedical data, using multiple machine learning algorithms. 
### Named Entity Recognition
* Given a corpus of texts, that consists of abstracts and titles and are related to the food and cosmetics, extract names of compounds that are not listed in our in-house dataset.
### Causality Inference
* Given a corpus of texts, that consists of abstracts and titles from papers and has to do with chemical compounds that are related to food and cosmetics industry, determine if the compound has a positive or negative relation to several adverse effects(cancer, neurotoxicity etc).

## Classification script (`classify.py`)

### Installation

* Create a new conda environment 
`conda create -n myEnv python=3.7.9`
* Activate conda environment 
`conda activate myEnv`
* Enter the root directory
`cd NLP_utils`
* Install dependencies:
`pip3 install -r requirements.txt`

## Python requirements
* `nltk==3.5`
* `pandas==1.2.0`
* `numpy==1.19.5`
* `scikit_learn==0.23.2`
* `tensorflow==2.4.1`

The training process can be bypassed. The pretrained models (LSVC.sav, LR.sav, SGD.sav, keras_model), vectorizer (vectorizer.pkl) and feature selector (selector.pkl) are provided [here](https://drive.google.com/drive/folders/1aLQkidYg-dO56aMkFZFsawwr_bcoLZqz) and should be placed in the root (NLP_utils) folder along with the `classify.py` script and `input.json` file.

## Usage
`cat input.json | python classify.py`

In the input.json each line is json entry of the form `{"id": "some form of id", "abstract": "Some abstract"}` and the ouput to the standard output is lines of json entries of the form: `{"id": "some form of id", "label": "relevant", "confidenceLevel": 0.52}`