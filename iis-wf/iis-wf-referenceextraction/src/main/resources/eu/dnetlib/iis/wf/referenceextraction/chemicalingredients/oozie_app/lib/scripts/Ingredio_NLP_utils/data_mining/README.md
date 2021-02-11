# Data mining
---
## Introduction
Obtaining and analyzing useful information from raw data is an essential process especially when this information is scattered within a vast amount of data. Our focus here was to extract data that correlate chemical substances found in food and cosmetics with cancer, toxicity, irritation and allergies from the OpenAIRE Research Graph data with the use of machine learning. 

## Methodology
In order to classify documents to those containing useful information about chemical substances and their relationship to cancer, irritation, allergies, and toxicity and those that do not, we used machine learning. To train the machine learning models a balanced training set was created, that consists of two main parts (classes): 1) Class A consists of peer-reviewed articles that describe information of how chemical ingredients of food and cosmetics are connected to allergies, irritation, cancer, and toxicity and 2) Class B consists of peer-reviewed articles that are irrelevant to the potential hazards related to food and cosmetics ingredients. We applied preprocessing to the dataset and used it to train four different classifiers, these classifiers are LinearSVC, Logistic Regression, SGD Classifier and Keras. After training the classifiers we used them to classify the documents we retrieved from the OpenAIRE Research Graph.

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
*    Create a `conda` environment named Phase2 and install python v3.7.9
`conda create -n Phase2 python=3.7.9`
*    Activate conda environment 
`conda activate Phase2`
*    Enter `data_mining` folder: `cd data_mining`
*    Install dependencies:
`pip3 install -r requirements.txt`
`conda install -c conda-forge pyicu morfessor icu -y && pip install pycld2 polyglot`

## Python requirements
* `keras_tuner==1.0.3`
* `matplotlib==3.3.3`
* `pandas==1.2.0`
* `venn==0.1.3`
* `tensorflow_gpu==2.0.0`
* `hpsklearn==0.0.3`
* `nltk==3.5`
* `numpy==1.19.5`
* `hyperopt==0.2.5`
* `dill==0.3.3`
* `multiprocess==0.70.6.1`
* `ipython==7.19.0`
* `scikit_learn==0.24.1`
* `tensorflow==2.4.1`
* `polyglot==16.7.4`

## Datasets
In order to create the training set multiple articles were retrieved as json entries from Ingredio database, Pubmed and Zenodo. The key tags that are used from those json entries are PMID, DOI, Abstract, Article. These entries are combined into one set. The resulting training set consists of two main parts (classes): 1) Class A consists of peer-reviewed articles that describe information of how chemical ingredients of food and cosmetics are connected to allergies, irritation, cancer, and toxicity and 2) Class B consists of peer-reviewed articles that are irrelevant to the potential hazards related to food and cosmetics ingredients. A sample of the training set can be seen below:


PMID|Abstract|DOI|target
-|-|-|-
10439772|Methylene blue, a soluble guanylyl cyclase inhibitor ...|10.1097/00000539-199908000-00045|1
25462712|Parabens are esters of para-hydroxybenzoic acid, with...|10.1016/j.watres.2014.09.030|1
32605666|Remote psychophysical evaluation of olfactory and gustatory...|10.1017/S0022215120001358|0

## Usage

* Step 1 - Creating the training set

`python create_trainingset.py`

This script retrieves the json entries from multiple files. It then creates one set for each class and adds labels to them. It adds the label 1 to the set that contains the relevant entries and the label 0 to the set that contains the irrelevant entries. Finally, it merges the two sets, shuffles them and exports it.


* Step 2 - Optimization & Training 

`python training.py`

The script first loads the dataset and converts the text to lowercase. It fits the TF-IDF vectorizer and the feature selector on the training set and exports them.
Then it uses the vectorizer to transform the text to numerical representation and feature selection to select the highest scoring features. Afterwards it uses Hyperopt to find the best hyperparameters for LinearSVC and SGD Classifier and saves both parameters and trained models. Then it uses GridSearchCV to find the best parameters for Logistic Regression and finally Hyperband to optimize Keras.


* Step 3 - Evaluation

`python model_evaluation.py`

This script loads the training set and split to train and test sets. Then it applies the same preprocessing as explained above and loads the best parameters for each model and fits each model on the training set. Afterwards, each model predicts the test set and returns the false predictions (false positives/negatives). It then creates a Venn diagram for both false positives and negatives. Next, it creates a voting classifier which combines the predictions of the classifiers on the test sets, creates classification_reports for each classifier and saves them locally. This procedure happens 15 times in a loop with different splits on the training set.
Finally, it imports the classification reports and plots them based on the precision.

* Step 4 - Predicting

`python predict.py`

The predict script first loads the trained models, the vectorizer and feature selector that are fitted on the training set. Then it loads the file that contains the documents that will be classified and applies preprocessing to them. Next it uses the vectorizer and feature selector to transform the text to numerical representation and select their highest scoring features. Afterwards, it uses the four classifiers to classify the documents and a custom voting classifier that combines the four votes. It finally exports the positively classified documents as json entries following the appropriate json schema.


