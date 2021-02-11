import pandas as pd
import json
import numpy as np
from polyglot.detect import Detector
import os

# Ingredio vars
data = []
pmid_data = []
relevantPMIDSList = ['all_categories','cosmetics_allergy','cosmetics_ingredient_safety','cosmetics_safety','cosmetics_toxicity','food_additive_safety','food_additive_toxicity','food_ingredient_safety','toxicity_food_additives']
irrelevantPMIDSList = ['medicinePMID','biologyPMID','chemistryPMID','drugsPMID','inhibitorPMID','cancer_treat_PMID','drug_toxicPMID']
relevant_folder = 'relevant/'
irrelevant_folder = 'irrelevant/'
rural = 'Rural_Digital_Europe'
transport = 'Transport_Research'
energy = 'Energy_Research'

def Ingredio_articles():   
    #Load Json to list
    with open('datasets/relevant/pmid_final.json') as f:
        for line in f:
            data.append(json.loads(line))
            
    #Retrieve PMID and meta data and add them to list
    for inner_l in data:
        for item in inner_l['pmid']:
            pmid_data.append(item)
    
    ingredioDF = pd.DataFrame(pmid_data)
    #Drop useless columns
    ingredioDF.drop(['Pubchem_ID', 'Journal'], axis = 1,inplace = True) 
    ingredioDF['Abstract'].replace('', np.nan, inplace=True)
    ingredioDF = ingredioDF[pd.notnull(ingredioDF['Abstract'])]
    ingredioDF['Abstract'] = ingredioDF['Article'] + " " + ingredioDF['Abstract']
    ingredioDF.drop(['Article'], axis = 1,inplace = True) 
    ingredioDF.dropna(how='any', inplace=True)
    return ingredioDF

ingredioDF = Ingredio_articles()
#Retrieve relevant/irrelevant from pubmed
def create_pubmedset(PMIDSList,folder):
    pmidDict = {}
    for pmid in PMIDSList:
        data = []
        pmid_data = []
        with open('datasets/'+folder + pmid + '.json') as f:
            for line in f:
                data.append(json.loads(line))
        #Retrieve PMID and meta data and add them to list
        for inner_l in data:
            for item in inner_l['pmid']:
                pmid_data.append(item)
        
        pmidDict[pmid] = pd.DataFrame(pmid_data)
        pmidDict[pmid].drop(['Journal'], axis = 1,inplace = True) 
        pmidDict[pmid]['Abstract'].replace('', np.nan, inplace=True)
        pmidDict[pmid] = pmidDict[pmid][pd.notnull(pmidDict[pmid]['Abstract'])]
        pmidDict[pmid]['Abstract'] = pmidDict[pmid]['Article'] + " " + pmidDict[pmid]['Abstract']
        pmidDict[pmid].drop(['Article'], axis = 1,inplace = True) 
    return pmidDict

relevantdict = create_pubmedset(relevantPMIDSList,relevant_folder)
irrelevantdict = create_pubmedset(irrelevantPMIDSList,irrelevant_folder)
#retrieve irrelevant from zenodo
def create_zenodoset(filename):
    data = []
    zenodo = []
    with open('datasets/irrelevant/' + filename + '.json', encoding='utf-8') as fr:
            for index,line in enumerate(fr):
                data.append(json.loads(line))
                if index == 5000:
                    break
    for l in data:
        try:
            if Detector(str(l['description'])).language.name == 'English':
                try:
                    zendict = {
                            "Article": l['maintitle'],
                            "DOI": l['pid'][0]['value'],
                            "Abstract": l['description']}
                    zenodo.append(zendict)
                except Exception as e:
                    pass
        except Exception as e:
            pass
            
    zenodoDF = pd.DataFrame(zenodo)
    zenodoDF['Abstract'] = zenodoDF['Abstract'].str[0]
    zenodoDF['Abstract'].replace('', np.nan, inplace=True)
    zenodoDF = zenodoDF[pd.notnull(zenodoDF['Abstract'])]
    zenodoDF['Abstract'] = zenodoDF['Article'] + " " + zenodoDF['Abstract']
    zenodoDF.drop(['Article'], axis = 1,inplace = True) 
    return zenodoDF

ruralDF = create_zenodoset(rural)
transportDF = create_zenodoset(transport)
energyDF = create_zenodoset(energy)

relevantList = [ingredioDF, relevantdict['all_categories'], relevantdict['cosmetics_allergy'],
                relevantdict['cosmetics_ingredient_safety'], relevantdict['cosmetics_safety'],
                relevantdict['cosmetics_toxicity'], relevantdict['food_additive_safety'],
                relevantdict['food_additive_toxicity'], relevantdict['food_ingredient_safety'], relevantdict['toxicity_food_additives']]
irrelevantList = [irrelevantdict['biologyPMID'], irrelevantdict['chemistryPMID'], irrelevantdict['drugsPMID'],
                  irrelevantdict['inhibitorPMID'], irrelevantdict['medicinePMID'], irrelevantdict['cancer_treat_PMID'], irrelevantdict['drug_toxicPMID'], energyDF, transportDF, ruralDF]
    
#create new dataframe from all random dataframes
irrelevantDF = pd.concat(irrelevantList,ignore_index=True)
relevantDF = pd.concat(relevantList,ignore_index=True)
#combine relevant/irrelevant dataframes into one
def combine_dataframes(df1,df2):
    df1.drop_duplicates(inplace=True)
    df2.drop_duplicates(inplace=True)
    df1 = df1.sample(frac=1).reset_index(drop=True)
    df2 = df2.sample(frac=1).reset_index(drop=True)
    df2 = df2.head(38000)
    df1['target'] = 1
    df2['target'] = 0
    combs = [df1,df2]
    combinedDF = pd.concat(combs,ignore_index=True)
    combinedDF = combinedDF.sample(frac=1).reset_index(drop=True)
    return combinedDF

combinedDF = combine_dataframes(relevantDF,irrelevantDF)
combinedDF.dropna(subset=['PMID'], how = 'all',inplace=True)


combinedDF.to_csv('datasets/trainingset.csv',index=False)
