import pandas as pd
import json
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.svm import SVC
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.linear_model import SGDClassifier
from sklearn.calibration import CalibratedClassifierCV
from tensorflow import keras
import matplotlib.pyplot as plt
plt.style.use('ggplot')
from venn import venn
import nltk
import pickle
import statistics
import matplotlib.pyplot as plt



stopwords = nltk.corpus.stopwords.words('english')
w_tokenizer = nltk.tokenize.WhitespaceTokenizer()
lemmatizer = nltk.stem.WordNetLemmatizer()
combinedDF = pd.read_csv('datasets/trainingset.csv',index_col=False)
combinedDF = combinedDF.sample(frac=0.002).reset_index(drop=True)
#Global Vars
models_list = ['LSVCparams.json','sgdParameters.json','LRParameters.json']
clf_list = []
false_pred_dict = {}
classifier_list = [SVC,SGDClassifier,LogisticRegression]

keras_model = keras.models.load_model('keras_model')

def lemmatize_text(text):
    return " ".join([lemmatizer.lemmatize(w) for w in w_tokenizer.tokenize(text)])
combinedDF['Abstr'] = combinedDF['Abstract'].str.lower().apply(lemmatize_text)
for iteration in range(1,15):
    classifier_nameList = ['LinearSVC','SGD','LogisticRegression','Keras']
    X, y, z, w = combinedDF['Abstr'], combinedDF['target'], combinedDF['PMID'], combinedDF['DOI']
    X_train, X_test, y_train, y_test, z_train, z_test, w_train, w_test = train_test_split(X ,y ,z ,w , test_size=0.2)
    
    #Scikit Learn Text feature selection
    selector = SelectKBest(chi2, k=5000)
    
    def create_testDF(X_test,y_test,z_test):
        testDF = pd.DataFrame()    
        testDF['Abstr'] = X_test
        testDF['target'] = y_test
        testDF['PMID'] = z_test
        testDF['DOI'] = w_test
        testDF.reset_index(inplace=True)
        testDF.drop('index', axis=1, inplace=True)
        testDF.to_csv('testDF.csv',index=False)
        return testDF
    
    testDF = create_testDF(X_test,y_test,z_test)
    
    def retrieve_params():
        param_list = []
        for model in models_list:
            #Retrieve model parameters
            with open(model) as json_file: 
                params = json.load(json_file)  
                param_list.append(params)
        json_file.close()
        return param_list
    
    
    def preproc(X_train,X_test):
        vectorizer = TfidfVectorizer(ngram_range=(2, 3), norm=None, smooth_idf=False, use_idf=False,stop_words='english')
        abstract_matrix = vectorizer.fit_transform(X_train)
        test_abstract_matrix = vectorizer.transform(X_test)
        abstract_matrix_selected = selector.fit_transform(abstract_matrix,y_train)
        test_abstract_matrix_selected = selector.transform(test_abstract_matrix)
    
        return abstract_matrix,test_abstract_matrix,abstract_matrix_selected,test_abstract_matrix_selected
    
    def train_classifiers(confidence_list):
        clf_list = []
        for i,classifier in enumerate(classifier_list):
            if classifier == SVC or classifier == SGDClassifier:
                LSVC =  classifier(**param_list[i])
                clf = CalibratedClassifierCV(LSVC)
                clf.fit(abstract_matrix_selected,y_train)
                clf_list.append(clf)
                testDF[classifier_nameList[i]] = clf.predict_proba(test_abstract_matrix_selected)[:,1].tolist()
                testDF[classifier_nameList[i]] = testDF[classifier_nameList[i]].apply(lambda x: 1 if x > confidence_list[i] else 0)
    
            else:
                clf = classifier(**param_list[i])
                clf.fit(abstract_matrix_selected,y_train)
                clf_list.append(clf)
                testDF[classifier_nameList[i]] = clf.predict_proba(test_abstract_matrix_selected)[:,1].tolist()
                testDF[classifier_nameList[i]] = testDF[classifier_nameList[i]].apply(lambda x: 1 if x > confidence_list[i] else 0)
                
        abstract_array = abstract_matrix_selected.toarray()
        history = keras_model.fit(abstract_array,y_train)
        clf_list.append(keras_model)
        testDF['Keras'] = keras_model.predict(test_abstract_matrix_selected)
        testDF['Keras'] = testDF['Keras'].apply(lambda x: 1 if x >0.58 else 0)
    
    fp_dict = {}
    fn_dict = {}
    def false_preds():
        for classifier in classifier_nameList:
            false_pred_dict["{0}_List".format(classifier)] = []
            fp_dict["{0}_List".format(classifier)] = []
            fn_dict["{0}_List".format(classifier)] = []
            for index, row in testDF.iterrows():
                if testDF['target'][index] == 1 and testDF[classifier][index] == 0:
                    fn_dict["{0}_List".format(classifier)].append(index)
                if testDF['target'][index] == 0 and testDF[classifier][index] == 1:
                    fp_dict["{0}_List".format(classifier)].append(index)
                if testDF['target'][index] != testDF[classifier][index]:
                    false_pred_dict["{0}_List".format(classifier)].append(index)
               
    param_list = retrieve_params()
    abstract_matrix,test_abstract_matrix,abstract_matrix_selected,test_abstract_matrix_selected = preproc(X_train,X_test)
    confidence_list = [0.48,0.45,0.52]
    
    train_classifiers(confidence_list)    
    false_preds()
    
    def draw_venn(m1,m2,m3,m4):
        common_sets = {
            "Keras": set(m1),
            "LinearSVC": set(m2),
            "LogisticRegression": set(m3),
            "SGD": set(m4)
        }
        venn(common_sets)    
    
    #Draw False Positive Venn Diagram
    draw_venn(fp_dict['Keras_List'],fp_dict['LinearSVC_List'],fp_dict['LogisticRegression_List'],fp_dict['SGD_List'])
    #Draw False Negative Venn Diagram
    draw_venn(fn_dict['Keras_List'],fn_dict['LinearSVC_List'],fn_dict['LogisticRegression_List'],fn_dict['SGD_List'])
    
    #Retrieve FN/FP from test set
    def common_false(diction):    
        found = []
        for item in diction['Keras_List']:
            if item in diction['LinearSVC_List']  and item in diction['SGD_List']:
                found.append(item)
        return found    
    
    fp_found = common_false(fp_dict) 
    fn_found = common_false(fn_dict)   
    fp_testDF  = testDF.iloc[fp_found]
    fn_testDF = testDF.iloc[fn_found]
    
    def voting_classifier(testDF):
        voting_list = []
        for index, row in testDF.iterrows():
            if row['LinearSVC'] + row['SGD'] + row['Keras'] + row['LogisticRegression'] > len(classifier_nameList) / 2:
                voting_list.append(1)
            elif row['LinearSVC'] + row['SGD'] + row['Keras'] + row['LogisticRegression'] == len(classifier_nameList) / 2:
                voting_list.append(row['LinearSVC'])
            else:
                voting_list.append(0)
        testDF['Voting'] = pd.Series(voting_list)
    voting_classifier(testDF)
    classifier_nameList.append('Voting')
    
    def classification_report(testDF,classifier_nameList):
        from sklearn.metrics import classification_report
        for i,classifier in enumerate(classifier_nameList):
            labels = [0,1]
            report = classification_report(testDF['target'].tolist(),testDF[classifier].tolist(),labels=labels, output_dict=True)
            reportDF = pd.DataFrame(report).transpose()
            reportDF.to_csv('classification_reports/{}report{}.csv'.format(classifier,iteration))
    
    classification_report(testDF,classifier_nameList)

classifier_nameList.append('Voting')
def import_classification_reports(classifier):
    precision_report_list = []
    for iteration in range(1,15):
        df = pd.read_csv('classification_reports/{}report{}.csv'.format(classifier,iteration))
        precision_report_list.append(df['precision'][1])
    return precision_report_list

class_reports_list = []
for i,classifier in enumerate(classifier_nameList):
    class_reports_list.append(import_classification_reports(classifier))
plt.figure()

for index,l in enumerate(class_reports_list):
    print(statistics.mean(l))
    plt.plot(l, label = classifier_nameList[index])
    plt.legend(loc="lower left", fontsize = 'small')
    plt.title("Model Precision")
    plt.ylabel("Precision")
    plt.xlabel("Trials")
plt.show()