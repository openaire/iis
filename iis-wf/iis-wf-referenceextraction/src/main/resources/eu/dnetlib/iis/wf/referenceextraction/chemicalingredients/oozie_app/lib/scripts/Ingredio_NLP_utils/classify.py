import sys
import json
import pandas as pd
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
from tensorflow import keras
import numpy as np
import nltk
import pickle
import math

def lemmatize_text(text):
    return " ".join([lemmatizer.lemmatize(w) for w in w_tokenizer.tokenize(text)])

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
    publicationdf.drop(['LinearSVC', 'SGD', 'Keras', 'LogisticRegression','abstract'], axis=1, inplace=True)
    return publicationdf

def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]
        
def truncate(f, n):
    return math.floor(f * 10 ** n) / 10 ** n

def prediction(input_,model_paths,tokenizer,lemmatizer,vectorizer,selector,classifier_list,confidence_list):
    publication = []
    for line in input_:
        try:
            input_toDict = json.loads(line)
            if input_toDict['abstract'] == '' or input_toDict['abstract'] == 'n/a':
                pass
            publication.append(input_toDict)
        except Exception as e:
            pass
    publicationdf = pd.DataFrame(publication)
    models_list=[]
    for path in model_paths:
        models_list.append(pickle.load(open(path, 'rb')))
    keras_model = keras.models.load_model('keras_model')
    models_list.append(keras_model)

    publicationdf['abstract'] = publicationdf['abstract'].str.lower().str.replace('-',' ').apply(lemmatize_text)
    publication_abstract_matrix = vectorizer.transform(publicationdf['abstract'])
    publication_abstract_matrix_selected = selector.transform(publication_abstract_matrix)

    #Predict with each classifier and add predictions to publications DF
    for i,model in enumerate(models_list):
        #if classifier is linear SVC
        if i == 0:
            publicationdf[classifier_list[i]] = model.predict(publication_abstract_matrix_selected)
        #If classifier is keras
        elif i==3:
            publicationdf[classifier_list[i]] = model.predict(publication_abstract_matrix_selected)
        else:
            publicationdf[classifier_list[i]] = model.predict_proba(publication_abstract_matrix_selected)[:,1]
    publicationdf['confidenceLevel'] = publicationdf.loc[:,['SGD','LogisticRegression','Keras']].mean(axis=1)
    
    publicationdf['confidenceLevel'] = publicationdf['confidenceLevel'].apply(lambda x: truncate(x, 2) if x > 0.5 else truncate(1-x, 2))
    for conf,classifier in zip(confidence_list,classifier_list):
        publicationdf[classifier] = publicationdf[classifier].apply(lambda x: 1 if x >conf else 0) 

    publicationdf = voting_classifier(publicationdf,classifier_list)
    publicationdf = publicationdf[['id', 'relevant', 'confidenceLevel']]
    pubs_dict = publicationdf.to_dict('records')
    for pub in pubs_dict:
        sys.stdout.write(str(pub) + '\n')

w_tokenizer = nltk.tokenize.WhitespaceTokenizer()
lemmatizer = nltk.stem.WordNetLemmatizer()
vectorizer = pickle.load(open("vectorizer.pkl", "rb"))
selector = pickle.load(open("selector.pkl", "rb"))
model_paths = ['LSVC.sav','SGD.sav','LR.sav']
classifier_list = ['LinearSVC','SGD','LogisticRegression','Keras']
confidence_list = [0.48,0.5,0.52,0.58]



if __name__ == "__main__":
    chunks = list(divide_chunks(sys.stdin.readlines(),1000))
    for chunk in chunks:
        prediction(chunk,model_paths,w_tokenizer,lemmatizer,vectorizer,selector,classifier_list,confidence_list)
