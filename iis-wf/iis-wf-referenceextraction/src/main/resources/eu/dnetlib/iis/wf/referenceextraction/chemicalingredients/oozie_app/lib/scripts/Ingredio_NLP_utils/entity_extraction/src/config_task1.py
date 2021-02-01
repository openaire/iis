import transformers
from transformers import BertTokenizer, BertConfig,BertForTokenClassification, AdamW
import torch

csv_file = "compoundDF_BERTReady2.csv"
MODEL_PATH = "entity_model_demo.bin"
MAX_LEN = 256
batch_size = 8
epochs = 3
max_grad_norm = 1.0
tag2idx = {0: 0, 1: 1, 'PAD': 2}
tag_values = [0, 1, 'PAD']
tokenizer = BertTokenizer.from_pretrained('monologg/biobert_v1.0_pubmed_pmc', do_lower_case=False)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
n_gpu = torch.cuda.device_count()
training_logs = 'training_logs_prototype'
learning_rate = 3e-5
eps = 1e-8
publications_filename = 'classified_publications/publications_export7.json'
ingredio_filename = 'new_with_g.json'
FULL_FINETUNING = True