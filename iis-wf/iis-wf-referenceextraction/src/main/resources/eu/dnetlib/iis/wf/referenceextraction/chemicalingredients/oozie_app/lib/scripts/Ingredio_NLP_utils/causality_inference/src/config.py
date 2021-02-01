import transformers

DEVICE = "cuda"
MAX_LEN=512
TRAIN_BATCH_SIZE = 8
VALID_BATCH_SIZE = 4
EPOCHS =1
BERT_PATH = '../input/bert_base_uncased/'
MODEL_PATH =  '../model.bin'
GENERATED = '../datasets/generated.csv'
CAUSAL = '../datasets/Causaly_small.csv'
TOKENIZER = transformers.BertTokenizer.from_pretrained(
    BERT_PATH,
    do_lower_case=True
)
