import dataset
import config
import engine
import pandas as pd
import numpy as np
from model import BERTBaseUncased
from sklearn import metrics
from sklearn.model_selection import train_test_split
from transformers import AdamW
from transformers import get_linear_schedule_with_warmup
import torch
import torch.nn as nn
import os
import random
import sys
os.environ["TOKENIZERS_PARALLELISM"] = "true"

def run():
    generated = pd.read_csv(config.GENERATED).fillna('none').values.tolist()
    causal = pd.read_csv(config.CAUSAL).fillna('none').values.tolist()
    # causal.extend(generated)
    print(*causal[:5],sep='\n')
    sys.exit()
    # causal = random.sample(causal,300)

    train,valid = train_test_split(
        causal,
        test_size=0.1,
        stratify = [z[1] for z in causal]
    )
    train_dataset = dataset.BERTDataset(
       sent  = [z[0] for z in train],
       label =  [z[1] for z in train]
    )
    train_dataloader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=config.TRAIN_BATCH_SIZE,
        num_workers=4
    )

    valid_dataset = dataset.BERTDataset(
       sent  = [z[0] for z in valid],
       label =  [z[1] for z in valid]
    )
    valid_dataloader = torch.utils.data.DataLoader(
        valid_dataset,
        batch_size=config.VALID_BATCH_SIZE,
        num_workers=1
    )

    device = torch.device(config.DEVICE)
    model = BERTBaseUncased()
    model.to(device)

    param_optimizer = list(model.named_parameters())
    no_decay = ['bias','LayerNorm.bias','LayerNorm.weight']
    optimizer_parameters = [
        {
            "params": [
                p for n, p in param_optimizer if not any(nd in n for nd in no_decay)
            ],
            "weight_decay": 0.001,
        },
        {
            "params": [
                p for n, p in param_optimizer if any(nd in n for nd in no_decay)
            ],
            "weight_decay": 0.0,
        },
    ]
    print(len(train))
    num_train_steps = int(len(train) / config.TRAIN_BATCH_SIZE * config.EPOCHS)
    optimizer = AdamW(optimizer_parameters, lr=3e-5)
    scheduler = get_linear_schedule_with_warmup(
        optimizer, num_warmup_steps=0, num_training_steps=num_train_steps
    )
    f = open("../classification_report.txt", "w")
    f.write("Classification Report\n")

    best_accuracy = 0
    for epoch in range(config.EPOCHS):
        engine.train_fn(train_dataloader, model, optimizer, device, scheduler)
        outputs, labels = engine.eval_fn(valid_dataloader, model, device)
        # outputs = np.array(outputs) >= 0.5

        outputs = [1 if x[0] else 0 for x in outputs]
        labels = [1 if x[0] > 0.5 else 0 for x in labels]
        print(metrics.classification_report(labels,outputs,[0,1]))

        f.write('Epoch: '+ str(epoch) + '\n')
        f.write(metrics.classification_report(labels,outputs,[0,1]))
        accuracy = metrics.accuracy_score(labels, outputs)
        print(f"Accuracy Score = {accuracy}")

        if accuracy > best_accuracy:
            torch.save(model.state_dict(), config.MODEL_PATH)
            best_accuracy = accuracy
    f.close()

if __name__ == "__main__":
    run()
