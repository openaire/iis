import json
import pandas as pd
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
from tensorflow import keras
import numpy as np
import nltk
import pickle
import multiprocess as mp
import gzip
from polyglot.detect import Detector
from polyglot.detect.base import logger as polyglot_logger
polyglot_logger.setLevel("ERROR")
import dill
dill.settings['recurse']=True

def save_classified(publicationdf,file):
    final = {}
    outfile = open('classified_outputs/' + file + '.json', "a")
    for index, row in publicationdf.iterrows():
        final['description'] = []
        final['externalReference'] = []
        final['id'] = ''
        final['pid'] = []
        final['resulttype'] = {}
        final['title'] = []
        strDOI = ''
        strPMID = ''
        qualifier = {"classid":"url","classname":"url","schemeid":"dnet:externalReference_typologies","schemename":"dnet:externalReference_typologies"}
        final['description'].append({'value': row['Abstract']})
        final['externalReference'].append({'qualifier': qualifier})
        final['id'] = row['ID']
        pidqualifier = {"classid": "doi","classname": "Digital Object Identifier","schemeid": "dnet:pid_types","schemename": "dnet:pid_types"}
        try:
            if(row['DOI'][0]['scheme'] == 'doi'):
                strDOI = row['DOI'][0]['value']
            if(row['DOI'][1]['scheme'] == 'pmid'):
                strPMID = row['DOI'][1]['value']    
            elif(row['DOI'][2]['scheme'] == 'pmid'):
                strPMID = row['DOI'][2]['value'] 
        except Exception as e:
            pass
        final['pid'].append({'qualifier': pidqualifier,'value': strDOI})
        if strPMID != '':
            pmidqualifier = {"classid": "pmid","classname": "PubMed ID","schemeid": "dnet:pid_types","schemename": "dnet:pid_types"}
            final['pid'].append({'qualifier':pmidqualifier,'value':strPMID})
        resulttype = {"classid": "publication","classname": "publication","schemeid": "dnet:result_typologies","schemename": "dnet:result_typologies"}
        final['resulttype'] = resulttype
        titlequalifier = {"classid": "main title","classname": "main title","schemeid": "dnet:dataCite_title","schemename": "dnet:dataCite_title"}
        final['title'].append({'qualifier': titlequalifier,'value':row['Article']})
        final['journal'] = {"name":row['Journal']}
        #Append to Json file 
        json.dump(final, outfile,separators=(',',':')) 
        outfile.write("\n")
    outfile.close()



def publication_DF(path):
    # Publications vars
    pubdata = []
    publication = []
    print(path)
    print(mp.current_process())
    with gzip.open('dumps/' + path, 'rb') as f:
        for line in f:
            pubdata.append(json.loads(line))
    for l in pubdata:
        try:
            if len(l['description']) > 0:
                for abstr in l['description']:
                    if Detector(abstr).language.name == 'English' and abstr != 'n/a':
                        if Detector(l['maintitle']).language.name == 'English':
                            title = l['maintitle']
                        else:
                            title = ''
                        try:
                            pubdict = {
                                    "Article": l['maintitle'],
                                    "Abstract": abstr,
                                    "Abstr": title + abstr,
                                    "DOI": l['pid'],
                                    "Journal": l['container']['name'],
                                    "ID": l['id']}
                            publication.append(pubdict)
                        except Exception as e:
                            try:
                                pubdict = {
                                        "Article": l['maintitle'],
                                        "Abstract": abstr,
                                        "Abstr": title + abstr,
                                        "DOI": l['pid'],
                                        "Journal": '',
                                        "ID": l['id']}
                                publication.append(pubdict)
                            except Exception as e:
                                pass
        except Exception as e:
            pass
    del pubdata
    publicationdf = pd.DataFrame(publication)
    del publication
    publicationdf['Abstr'].replace('n/a', np.nan, inplace=True)
    publicationdf = publicationdf[pd.notnull(publicationdf['Abstr'])]
    publicationdf['Abstr'] = publicationdf['Abstr'].str.lower().str.replace('-',' ').apply(lemmatize_text)
    return publicationdf

def lemmatize_text(text):
    lemmatizer = nltk.stem.WordNetLemmatizer()
    tokenizer = nltk.tokenize.WhitespaceTokenizer()
    return " ".join([lemmatizer.lemmatize(w) for w in tokenizer.tokenize(text)])

def voting_classifier(publicationdf,classifier_list):
    voting_list = []
    for index, row in publicationdf.iterrows():
        if row['LinearSVC'] + row['SGD'] + row['Keras'] + row['LogisticRegression'] > len(classifier_list) / 2:
            voting_list.append(1)
        elif row['LinearSVC'] + row['SGD'] + row['Keras'] + row['LogisticRegression'] == len(classifier_list) / 2:
            if row['LinearSVC'] == 1:
                voting_list.append(1)
            else:
                voting_list.append(0)
        else:
            voting_list.append(0)
    publicationdf['Voting'] = pd.Series(voting_list)
    publicationdf.drop(publicationdf[publicationdf.Voting == 0].index, inplace=True)
    return publicationdf

def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

def initialize_models(model_paths):
    models_list=[]
    for path in model_paths:
        models_list.append(pickle.load(open(path, 'rb')))
    keras_model = keras.models.load_model('keras_model')
    models_list.append(keras_model)
    return models_list

def prediction(file):
    publicationdf = publication_DF(file)
    publication_abstract_matrix = vectorizer.transform(publicationdf['Abstr'])
    publication_abstract_matrix_selected = selector.transform(publication_abstract_matrix)

    for i,model in enumerate(models_list):
        if i == 3:
            publicationdf['Keras'] = model.predict(publication_abstract_matrix_selected.toarray())
            publicationdf['Keras'] = publicationdf['Keras'].apply(lambda x: 1 if x >0.58 else 0)
        else:
            publicationdf[classifier_list[i]] = model.predict(publication_abstract_matrix_selected)

    publicationdf = voting_classifier(publicationdf,classifier_list)
    save_classified(publicationdf,file)

model_paths = ['LSVC.sav','SGD.sav','LR.sav']
classifier_list = ['LinearSVC','SGD','LogisticRegression','Keras']
vectorizer = pickle.load(open("vectorizer.pkl", "rb"))
selector = pickle.load(open("selector.pkl", "rb"))
models_list = initialize_models(model_paths)
file_list = []
for file in os.listdir('dumps'):
    if file.endswith(".gz"):
        file_list.append(file)

if __name__ == "__main__":

    with mp.Pool(2) as p:
        p.map(prediction,file_list)