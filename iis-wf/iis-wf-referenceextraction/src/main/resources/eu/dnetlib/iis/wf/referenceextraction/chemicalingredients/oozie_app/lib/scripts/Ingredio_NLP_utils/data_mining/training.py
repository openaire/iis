import pandas as pd
import json
import nltk
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from hyperopt import tpe
import IPython
import matplotlib.pyplot as plt
from hpsklearn import HyperoptEstimator
from hpsklearn import sgd,svc_linear
plt.style.use('ggplot')
import pickle


w_tokenizer = nltk.tokenize.WhitespaceTokenizer()
lemmatizer = nltk.stem.WordNetLemmatizer()
#Scikit Learn Text feature selection 
selector = SelectKBest(chi2, k=5000)
classifierList = [sgd,svc_linear]
LSVCpath = 'LSVCparams'
SGDpath = 'sgdParameters'
LRpath = 'LRParameters'
models_list = ['SGD.sav','LSVC.sav']

LRparam_dist = {
                  'penalty': ['none', 'l1', 'l2', 'elasticnet'],
                  'C': [100, 10, 1.0, 0.1, 0.01],
                  'solver': ['newton-cg','lbfgs','liblinear','sag','saga']
                  }
pathList = [SGDpath,LSVCpath]

#TFIDF Vectorizer init
vectorizer = TfidfVectorizer(ngram_range=(2, 3), norm=None, smooth_idf=False, use_idf=False,stop_words='english')

combinedDF = pd.read_csv('datasets/trainingset.csv',index_col=False)
def preprocess_text(dataframe,column_name,target,selector,lemmatizer,vectorizer,tokenizer,pickle_file,selector_pickle,train = True):
    dataframe[column_name] = dataframe[column_name].str.replace(r"[-]+", " ")
    if train:
        dataframe[column_name] = dataframe[column_name].str.lower().apply(lemmatize_text)
        vectorizer.fit(dataframe[column_name])
        pickle.dump(vectorizer,open(pickle_file,"wb"))
        del vectorizer
        vectorizer = pickle.load(open(pickle_file, "rb"))
        vectorized_matrix = vectorizer.transform(dataframe[column_name])
        selector.fit(vectorized_matrix,dataframe[target])
        pickle.dump(selector,open(selector_pickle,"wb"))
        del selector
        selector = pickle.load(open(selector_pickle, "rb"))
        vectorized_matrix_selected = selector.transform(vectorized_matrix)       
    else:
        dataframe[column_name] = dataframe[column_name].str.lower().apply(lemmatize_text)
        vectorizer = pickle.load(open(pickle_file, "rb"))    
        selector = pickle.load(open(selector_pickle, "rb"))
        vectorized_matrix = vectorizer.transform(dataframe[column_name])
        vectorized_matrix_selected = selector.fit_transform(vectorized_matrix,dataframe[target])            
    
    return vectorized_matrix_selected


def lemmatize_text(text):
    return " ".join([lemmatizer.lemmatize(w) for w in w_tokenizer.tokenize(text)])



def find_best_params(X,y,classifier,index):
    estim = HyperoptEstimator(classifier=classifier('classifier'),
                          preprocessing=[],
                          algo=tpe.suggest,
                          n_jobs = -1)
    estim.fit(X, y)
    clf = estim.best_model()['learner']
    diction = {}
    diction = clf.get_params()
    diction.pop('random_state', None)
    clf.fit(X,y)
    pickle.dump(clf, open(models_list[index], 'wb'))
    return diction


def save_found_params(parameters,path):
    with open(path + ".json", "w") as outfile:  
        json.dump(parameters, outfile)    
        
def create_testDF(X_test,y_test,z_test):
    testDF = pd.DataFrame()
    
    testDF['Abstr'] = X_test
    testDF['target'] = y_test
    testDF['ID'] = z_test
    testDF.reset_index(inplace=True)
    testDF.drop('index', axis=1, inplace=True)
    return testDF

if __name__ == '__main__':
    vectorized_matrix_selected = preprocess_text(combinedDF,'Abstract','target',selector,lemmatizer,vectorizer,w_tokenizer,'vectorizer.pkl','selector.pkl',True)
    #Convert to arrays for Keras
    abstract_array = vectorized_matrix_selected.toarray()
    
    
    for index,classifier in enumerate(classifierList):
        param_dict = find_best_params(vectorized_matrix_selected,combinedDF['target'],classifier,index)
        save_found_params(param_dict, pathList[index])
    
    #LOGISTIC REGRESSION GRID SEARCH
    LR = LogisticRegression()
    LRGS = GridSearchCV(LR, LRparam_dist)
    best_model = LRGS.fit(vectorized_matrix_selected, combinedDF['target'])
    diction = best_model.best_params_
    save_found_params(diction,LRpath)
    pickle.dump(best_model, open('LR.sav', 'wb'))
    
    #Import Keras
    from tensorflow import keras
    from tensorflow.keras.layers import Conv2D, MaxPooling2D, Dense, Flatten, Activation, Dropout
    from tensorflow.keras import layers
    from kerastuner.tuners import Hyperband
    
    #Keras
    LOG_DIR = f"{int(time.time())}"
    es_callback = keras.callbacks.EarlyStopping(monitor='val_loss', patience=3)
    
    class ClearTrainingOutput(keras.callbacks.Callback):
      def on_train_end(*args, **kwargs):
        IPython.display.clear_output(wait = True)
    
    def build_model(hp):
        keras_model = keras.Sequential()
        keras_model.add(layers.Dense(hp.Int("input_units",32,512,32), input_shape=(5000,)))
        keras_model.add(Dropout(hp.Float('dropout_input',min_value=0.0, max_value=0.4,step=0.1,default=0.4)))
        keras_model.add(Activation('relu'))
        for i in range(hp.Int("n_layers",1,3)):
            keras_model.add(layers.Dense(hp.Int(f'conv_{i}_units',32,512,32)))
            keras_model.add(Activation('relu'))
            keras_model.add(Dropout(hp.Float(f'dropout_{i}_layer',min_value=0.0, max_value=0.4,step=0.1,default=0.4)))
        keras_model.add(layers.Dense(1, activation='sigmoid'))
        keras_model.compile(optimizer = keras.optimizers.Adam(),loss='binary_crossentropy',metrics=['accuracy'])
        return keras_model
    
    tuner = Hyperband(
        build_model,
        max_epochs=45,
        factor = 3,
        objective='val_accuracy',
        executions_per_trial=2,
        directory=LOG_DIR
    )
    
    tuner.search(x=abstract_array,
                 y=combinedDF['target'],
                 epochs=45,
                 callbacks=[es_callback,ClearTrainingOutput()],
                 validation_split = 0.2,
                 use_multiprocessing=True)
    
    best_hps = tuner.get_best_hyperparameters(num_trials = 1)[0]
    print(best_hps.values)
    keras_model = tuner.hypermodel.build(best_hps)
    keras_model.fit(abstract_array,combinedDF['target'])
    keras_model.save('keras_model')









