import config
import torch

class BERTDataset:
    def __init__(self,sent,label):
        self.sent = sent
        self.label = label
        self.tokenizer = config.TOKENIZER
        self.max_len = config.MAX_LEN

    def __len__(self):
        return len(self.sent)

    def __getitem__(self,item):
         sent = str(self.sent[item])
         sent = " ".join(sent.split())

         inputs = self.tokenizer.encode_plus(
             sent,
             None,
             add_special_tokens = True,
             max_length=self.max_len,
             truncation=True,
             pad_to_max_length=True
         )

         ids = inputs['input_ids']
         mask = inputs['attention_mask']
         token_type_ids = inputs['token_type_ids']

        #  padding_length =self.max_len - len(ids)
        #  ids = ids + ([0] * padding_length)
        #  mask = mask + ([0] * padding_length)
        #  token_type_ids = token_type_ids + ([0] * padding_length)

         return {
            'ids': torch.tensor(ids, dtype=torch.long),
            'mask': torch.tensor(mask, dtype=torch.long),
            'token_type_ids': torch.tensor(token_type_ids, dtype=torch.long),
            'labels': torch.tensor(self.label[item], dtype=torch.float),
         }
