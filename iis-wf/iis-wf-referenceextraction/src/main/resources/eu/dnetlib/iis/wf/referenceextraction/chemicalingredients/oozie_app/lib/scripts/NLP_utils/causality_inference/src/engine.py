import torch
import torch.nn as nn
from tqdm import tqdm

def loss_fn(outputs, targets):
    return nn.BCEWithLogitsLoss()(outputs, targets.view(-1, 1))

def train_fn(data_loader, model, optimizer, device, scheduler):
    model.train()

    for bi, d in tqdm(enumerate(data_loader), total=len(data_loader)):
        ids = d["ids"]
        token_type_ids = d["token_type_ids"]
        mask = d["mask"]
        labels = d["labels"]

        token_type_ids = token_type_ids.to(device,dtype=torch.long)
        ids = ids.to(device,dtype=torch.long)
        mask = mask.to(device,dtype=torch.long)
        labels = labels.to(device,dtype=torch.float)

        optimizer.zero_grad()
        outputs = model(
            ids=ids,
            mask=mask,
            token_type_ids=token_type_ids
        )
        loss = loss_fn(outputs,labels)
        loss.backward()
        optimizer.step()
        scheduler.step()

def eval_fn(data_loader,model,device):
    model.eval()
    fin_labels=[]
    fin_outputs=[]
    with torch.no_grad():
        for batch_index , d in tqdm(enumerate(data_loader),total=len(data_loader)):
            ids = d['ids']
            token_type_ids = d['token_type_ids']
            mask = d['mask']
            labels = d['labels']

            token_type_ids = token_type_ids.to(device,dtype=torch.long)
            ids = ids.to(device,dtype=torch.long)
            mask = mask.to(device,dtype=torch.long)
            labels = labels.to(device,dtype=torch.float)

            outputs = model(
                ids=ids,
                mask=mask,
                token_type_ids=token_type_ids
            )
            fin_labels.extend(labels.cpu().detach().numpy().tolist())
            fin_outputs.extend(torch.sigmoid(outputs).cpu().detach().numpy().tolist())

    return fin_outputs,fin_outputs
