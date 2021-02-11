from transformers import BertForTokenClassification, AdamW
import config_task1 as config



def init_model():
  model = BertForTokenClassification.from_pretrained(
  "monologg/biobert_v1.0_pubmed_pmc",
  num_labels=len(config.tag2idx),
  output_attentions = False,
  output_hidden_states = False
  )
  return model