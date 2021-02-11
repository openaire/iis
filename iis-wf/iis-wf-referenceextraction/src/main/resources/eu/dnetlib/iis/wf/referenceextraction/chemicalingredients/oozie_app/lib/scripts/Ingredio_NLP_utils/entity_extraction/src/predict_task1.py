import config_task1 as config
import torch
import numpy as np
import find_compounds_task1 as find_compounds
import model_task1 as model
import sys

#Initialize model
model = model.init_model()
#Load best model
model.load_state_dict(torch.load("entity_model.bin"))
#Send to GPU
model.cuda();

tag_values = config.tag_values
publications_filename = config.publications_filename

test_sentence = """
The contact allergen dinitrochlorobenzene (DNCB) and respiratory allergy in the Th2-prone Brown Norway rat
"""

def predict(test_sentence,model,tag_values):
    tokenized_sentence = config.tokenizer.encode(test_sentence)
    input_ids = torch.tensor([tokenized_sentence]).cuda()
    
    with torch.no_grad():
        output = model(input_ids)
    label_indices = np.argmax(output[0].to('cpu').numpy(), axis=2)
    
    # join bpe split tokens
    tokens = config.tokenizer.convert_ids_to_tokens(input_ids.to('cpu').numpy()[0])
    new_tokens, new_labels = [], []
    for token, label_idx in zip(tokens, label_indices[0]):
        if token.startswith("##"):
            new_tokens[-1] = new_tokens[-1] + token[2:]
        else:
            new_labels.append(tag_values[label_idx])
            new_tokens.append(token)
    
    for token, label in zip(new_tokens, new_labels):
        print("{}\t{}".format(label, token))
    
    for tok,lab in zip(new_tokens,new_labels):
      if lab == 1:
        print(tok,lab)

predict(test_sentence,model,tag_values)

description_list = find_compounds.retrieve_classified_publications(publications_filename)

comps_found = find_compounds.find_compounds(description_list,model,tag_values)

print(comps_found)

syns_list = find_compounds.ingredio_DB('new_with_g.json')

print(len(list(set(comps_found) - set(syns_list))))




