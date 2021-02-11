import pandas as pd
import numpy as np
from tqdm import tqdm, trange
from sklearn.model_selection import train_test_split
import torch
import config_task1 as config
from tensorflow.keras.preprocessing.sequence import pad_sequences
from torch.utils.data import TensorDataset, DataLoader, RandomSampler, SequentialSampler
from transformers import BertForTokenClassification, AdamW, get_linear_schedule_with_warmup
import find_compounds_task1 as find_compounds

class SentenceGetter(object):

    def __init__(self, data):
        self.n_sent = 1
        self.data = data
        self.empty = False
        agg_func = lambda s: [(w, p, t) for w, p, t in zip(s["Word"].values.tolist(),
                                                           s["POS"].values.tolist(),
                                                           s["Tag"].values.tolist())]
        self.grouped = self.data.groupby("Sentence #").apply(agg_func)
        self.sentences = [s for s in self.grouped]

    def get_next(self):
        try:
            s = self.grouped["Sentence: {}".format(self.n_sent)]
            self.n_sent += 1
            return s
        except:
            return None
            
def create_dataset(csv_file):
    #Load CSV
    data = pd.read_csv(csv_file, encoding="latin1").fillna(method="ffill")
    
    data.drop(['Unnamed: 0'], axis=1, inplace=True)
    data.dropna(how='any', inplace=True)
           
    getter = SentenceGetter(data)
    #Group into sentences
    sentences = [[word[0] for word in sentence] for sentence in getter.sentences]
    
    labels = [[s[2] for s in sentence] for sentence in getter.sentences]
    
    tag_values = list(set(data["Tag"].values))
    tag_values.append("PAD")
    tag2idx = {t: i for i, t in enumerate(tag_values)}
    
    #Tokenize sentences
    def tokenize_and_preserve_labels(sentence, text_labels):
        tokenized_sentence = []
        labels = []
    
        for word, label in zip(sentence, text_labels):
    
            # Tokenize the word and count # of subwords the word is broken into
            tokenized_word = config.tokenizer.tokenize(word)
            n_subwords = len(tokenized_word)
    
            # Add the tokenized word to the final tokenized word list
            tokenized_sentence.extend(tokenized_word)
    
            # Add the same label to the new list of labels `n_subwords` times
            labels.extend([label] * n_subwords)
    
        return tokenized_sentence, labels
    
    tokenized_texts_and_labels = [
        tokenize_and_preserve_labels(sent, labs)
        for sent, labs in zip(sentences, labels)
    ]
    
    tokenized_texts = [token_label_pair[0] for token_label_pair in tokenized_texts_and_labels]
    labels = [token_label_pair[1] for token_label_pair in tokenized_texts_and_labels]
    
    #Pad or cut sequences
    input_ids = pad_sequences([config.tokenizer.convert_tokens_to_ids(txt) for txt in tokenized_texts],
                              maxlen=config.MAX_LEN, dtype="long", value=0.0,
                              truncating="post", padding="post")
    
    tags = pad_sequences([[tag2idx.get(l) for l in lab] for lab in labels],
                         maxlen=config.MAX_LEN, value=tag2idx["PAD"], padding="post",
                         dtype="long", truncating="post")
    
    attention_masks = [[float(i != 0.0) for i in ii] for ii in input_ids]
    
    #Train test split
    tr_inputs, val_inputs, tr_tags, val_tags = train_test_split(input_ids, tags,
                                                                random_state=2018, test_size=0.1)
    tr_masks, val_masks, _, _ = train_test_split(attention_masks, input_ids,
                                                 random_state=2018, test_size=0.1)
    
    tr_inputs, test_inputs, tr_tags, test_tags, tr_masks, test_masks = train_test_split(tr_inputs, tr_tags, tr_masks,
                                                                random_state=2018, test_size=0.1)

    #Convert to tensors                                                            
    tr_inputs = torch.tensor(tr_inputs)
    val_inputs = torch.tensor(val_inputs)
    tr_tags = torch.tensor(tr_tags)
    val_tags = torch.tensor(val_tags)
    tr_masks = torch.tensor(tr_masks)
    val_masks = torch.tensor(val_masks)

    test_inputs = torch.tensor(test_inputs)
    test_tags = torch.tensor(test_tags)
    test_masks = torch.tensor(test_masks)
    
    train_data = TensorDataset(tr_inputs, tr_masks, tr_tags)
    train_sampler = RandomSampler(train_data)
    train_dataloader = DataLoader(train_data, sampler=train_sampler, batch_size=config.batch_size)
    
    valid_data = TensorDataset(val_inputs, val_masks, val_tags)
    valid_sampler = SequentialSampler(valid_data)
    valid_dataloader = DataLoader(valid_data, sampler=valid_sampler, batch_size=config.batch_size)
    
    test_data = TensorDataset(test_inputs, test_masks, test_tags)
    test_sampler = RandomSampler(test_data)
    test_dataloader = DataLoader(train_data, sampler=train_sampler, batch_size=config.batch_size)
    return train_dataloader, valid_dataloader, test_dataloader, tag_values, tag2idx

