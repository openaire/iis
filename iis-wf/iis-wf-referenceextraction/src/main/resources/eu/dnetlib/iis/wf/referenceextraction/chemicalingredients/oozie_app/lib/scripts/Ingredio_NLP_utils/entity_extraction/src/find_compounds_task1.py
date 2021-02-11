import json
import nltk
nltk.download('punkt')
import config_task1 as config
import torch
import numpy as np

#Get publications
def retrieve_classified_publications(path):
    #Create a list to append the classified publications
    publications_list = []
    with open(path, 'r') as f:   
        for line in f:
            #Append each publication as a dictionary to the list
            publications_list.append(json.loads(line))
    f.close()
    print("Retrieving classified publication articles")
    description_list = []
    for row in publications_list:
        #Append article title, DOI, abstract of each classified article to a list
        description_list.append(row['title'][0]['value'] + row['description'][0]['value'])
    return description_list

def find_compounds(description_list,model,tag_values):
    total_labels = []
    total_tokens = []
    new_tokens, new_labels = [], []
    for index,descr in enumerate(description_list):
      print(index)
      if index == 50:
        break
      else:
        for sentence in nltk.sent_tokenize(descr):
          tokenized_sentence = config.tokenizer.encode(sentence)
          input_ids = torch.tensor([tokenized_sentence]).cuda()
    
          with torch.no_grad():
              output = model(input_ids)
          label_indices = np.argmax(output[0].to('cpu').numpy(), axis=2)
    
          # join bpe split tokens
          tokens = config.tokenizer.convert_ids_to_tokens(input_ids.to('cpu').numpy()[0])
          
          for token, label_idx in zip(tokens, label_indices[0]):
              if token.startswith("##"):
                  new_tokens[-1] = new_tokens[-1] + token[2:]
              else:
                  new_labels.append(tag_values[label_idx])
                  new_tokens.append(token)
    comps_found = set()
    for token, label in zip(new_tokens, new_labels):
        if label == 1:
          if len(token) > 3 and not token.isdigit():
            comps_found.add(token.lower())
    return comps_found

#Load Ingredio DB and return all compounds
def ingredio_DB(path):
    with open(path, encoding="utf8") as json_file:
        data = json.load(json_file)
    json_file.close()
    ingredio_dict = data['Ingredients_withFoods']
    syns_list = []
    for item in ingredio_dict:
      for syn in item['compName']:
        syns_list.append(syn)
    return syn


