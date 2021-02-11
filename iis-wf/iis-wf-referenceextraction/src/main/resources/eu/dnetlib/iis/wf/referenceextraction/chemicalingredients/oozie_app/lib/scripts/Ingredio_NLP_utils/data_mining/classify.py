import sys
import json
import pandas as pd
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
from tensorflow import keras
import numpy as np
import nltk
import pickle
import statistics
from multiprocessing import Pool

def lemmatize_text(text):
    lemmatizer = nltk.stem.WordNetLemmatizer()
    tokenizer = nltk.tokenize.WhitespaceTokenizer()
    return " ".join([lemmatizer.lemmatize(w) for w in tokenizer.tokenize(text)])

def voting_classifier(publicationdf,classifier_list):
    voting_list = []
    for index, row in publicationdf.iterrows():
        if row['LinearSVC'] + row['SGD'] + row['Keras'] + row['LogisticRegression'] > len(classifier_list) / 2:
            voting_list.append('true')
        elif row['LinearSVC'] + row['SGD'] + row['Keras'] + row['LogisticRegression'] == len(classifier_list) / 2:
            if row['LinearSVC'] == 1:
                voting_list.append('true')
            else:
                voting_list.append('false')
        else:
            voting_list.append('false')
            publicationdf['relevant'] = pd.Series(voting_list)
            #publicationdf.drop(['LinearSVC', 'SGD', 'Keras', 'LogisticRegression','abstract'], axis=1, inplace=True)
    return publicationdf

def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

def prediction(input_):
    vectorizer = pickle.load(open("test_vectorizer.pkl", "rb"))
    selector = pickle.load(open("test_selector.pkl", "rb"))
    model_paths = ['LSVC.sav','SGD.sav','LR.sav']
    classifier_list = ['LinearSVC','SGD','LogisticRegression','Keras']
    publication = []
    for line in input_:
        try:
            input_toDict = json.loads(line)
            publication.append(input_toDict)
        except Exception as e:
            pass
    publicationdf = pd.DataFrame(publication)
    models_list=[]
    for path in model_paths:
        models_list.append(pickle.load(open(path, 'rb')))
    keras_model = keras.models.load_model('keras_model1')
    models_list.append(keras_model)

    publicationdf['abstract'] = publicationdf['abstract'].str.lower().str.replace('-',' ').apply(lemmatize_text)
    publication_abstract_matrix = vectorizer.transform(publicationdf['abstract'])
    publication_abstract_matrix_selected = selector.transform(publication_abstract_matrix)

    for i,model in enumerate(models_list):
        if i == 3:
            publicationdf['Keras'] = model.predict(publication_abstract_matrix_selected.toarray())
            publicationdf['Keras'] = publicationdf['Keras'].apply(lambda x: 1 if x >0.58 else 0)
        else:
            publicationdf[classifier_list[i]] = model.predict(publication_abstract_matrix_selected)

    publicationdf = voting_classifier(publicationdf,classifier_list)
    pubs_dict = publicationdf.to_dict('records')
    for pub in pubs_dict:
        sys.stdout.write(str(pub) + '\n')

if __name__ == "__main__":
    chunks = list(divide_chunks(sys.stdin.readlines(),1000))
    with Pool(7) as p:
        p.map(prediction,chunks)
#    for chunk in chunks:
#        prediction(chunk)
