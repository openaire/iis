import config_task1 as config
import statistics
from tqdm import tqdm, trange
import torch
from transformers import BertForTokenClassification, AdamW, get_linear_schedule_with_warmup
import numpy as np
from sklearn.metrics import classification_report, precision_score, f1_score
import sys
import dataset_task1 as dataset
import model_task1 as model
import pandas as pd

#Retrieve the dataloaders created from the csv
train_dataloader,valid_dataloader,test_dataloader,tag_values, tag2idx = dataset.create_dataset(config.csv_file)

f = open(config.training_logs, 'a')
ep = 1
#Model initialization
model = model.init_model()
#Send model to gpu
model.cuda();

#Finetuning setup
if config.FULL_FINETUNING:
    param_optimizer = list(model.named_parameters())
    no_decay = ['bias', 'gamma', 'beta']
    optimizer_grouped_parameters = [
        {'params': [p for n, p in param_optimizer if not any(nd in n for nd in no_decay)],
        'weight_decay_rate': 0.01},
        {'params': [p for n, p in param_optimizer if any(nd in n for nd in no_decay)],
        'weight_decay_rate': 0.0}
    ]
else:
    param_optimizer = list(model.classifier.named_parameters())
    optimizer_grouped_parameters = [{"params": [p for n, p in param_optimizer]}]

optimizer = AdamW(
    optimizer_grouped_parameters,
    lr = config.learning_rate,
    eps = config.eps
)

# Total number of training steps is number of batches * number of epochs.
total_steps = len(train_dataloader) * config.epochs

# Create the learning rate scheduler.
scheduler = get_linear_schedule_with_warmup(
    optimizer,
    num_warmup_steps=0,
    num_training_steps=total_steps
)
loss_values, validation_loss_values = [], []
best_precision = 0

for _ in trange(config.epochs, desc="Epoch"):
    #               Training

    # Put the model into training mode.
    model.train()
    # Reset the total loss for this epoch.
    total_loss = 0

    # Training loop
    for step, batch in enumerate(train_dataloader):
        # add batch to gpu
        batch = tuple(t.to(config.device) for t in batch)
        b_input_ids, b_input_mask, b_labels = batch
        # Always clear any previously calculated gradients before performing a backward pass.
        model.zero_grad()
        # forward pass
        # This will return the loss (rather than the model output)
        # because we have provided the `labels`.
        outputs = model(b_input_ids, token_type_ids=None,
                        attention_mask=b_input_mask, labels=b_labels)
        # get the loss
        loss = outputs[0]
        # Perform a backward pass to calculate the gradients.
        loss.backward()
        # track train loss
        total_loss += loss.item()
        # Clip the norm of the gradient
        # This is to help prevent the "exploding gradients" problem.
        torch.nn.utils.clip_grad_norm_(parameters=model.parameters(), max_norm=config.max_grad_norm)
        # update parameters
        optimizer.step()
        # Update the learning rate.
        scheduler.step()

    # Calculate the average loss over the training data.
    avg_train_loss = total_loss / len(train_dataloader)
    print("Average train loss: {}".format(avg_train_loss))

    # Store the loss value for plotting the learning curve.
    loss_values.append(avg_train_loss)

    #               Validation

    # After the completion of each training epoch, measure our performance on
    # our validation set.

    # Put the model into evaluation mode
    model.eval()
    # Reset the validation loss for this epoch.
    eval_loss, eval_accuracy = 0, 0
    nb_eval_steps, nb_eval_examples = 0, 0
    predictions , true_labels = [], []
    for batch in valid_dataloader:
      batch = tuple(t.to(config.device) for t in batch)
      b_input_ids, b_input_mask, b_labels = batch

      # Telling the model not to compute or store gradients,
      # saving memory and speeding up validation
      with torch.no_grad():
          # Forward pass, calculate logit predictions.
          # This will return the logits rather than the loss because we have not provided labels.
          outputs = model(b_input_ids, token_type_ids=None,
                          attention_mask=b_input_mask, labels=b_labels)
      # Move logits and labels to CPU
      logits = outputs[1].detach().cpu().numpy()
      label_ids = b_labels.to('cpu').numpy()

      # Calculate the accuracy for this batch of test sentences.
      eval_loss += outputs[0].mean().item()
      predictions.extend([list(p) for p in np.argmax(logits, axis=2)])
      true_labels.extend(label_ids)
      batch_pred = [[list(p) for p in np.argmax(logits, axis=2)]]
      batch_true = [label_ids]

    eval_loss = eval_loss / len(valid_dataloader)
    validation_loss_values.append(eval_loss)
    print("Validation loss: {}".format(eval_loss))
    pred_tags = [tag_values[p_i] for p, l in zip(predictions, true_labels)
                                 for p_i, l_i in zip(p, l) if tag_values[l_i] != "PAD"]
    valid_tags = [tag_values[l_i] for l in true_labels
                                  for l_i in l if tag_values[l_i] != "PAD"]
    precision = precision_score(valid_tags, pred_tags)
    f1 = f1_score(pred_tags, valid_tags)
    f.write('Epoch No {} \n'.format(ep))
    print("Precision : {}".format(precision))
    print("Validation F1-Score: {}".format(f1))
    report = classification_report(valid_tags, pred_tags, output_dict=True)
    reportDF = pd.DataFrame(report).transpose()
    f.write(
      reportDF.to_string(header = True, index = False)
    )
    f.write('\n')

    ep += 1

    if precision > best_precision:
      best_precision = precision
      torch.save(model.state_dict(), config.MODEL_PATH)
f.close()

#Load the best model
model.load_state_dict(torch.load(config.MODEL_PATH))

#Evaluation on test set
f = open(config.training_logs, 'a')
validation_loss_values = []
eval_loss, eval_accuracy = 0, 0
nb_eval_steps, nb_eval_examples = 0, 0
predictions , true_labels = [], []
for batch in test_dataloader:
    batch = tuple(t.to(config.device) for t in batch)
    b_input_ids, b_input_mask, b_labels = batch

    # Telling the model not to compute or store gradients,
    # saving memory and speeding up validation
    with torch.no_grad():
        # Forward pass, calculate logit predictions.
        # This will return the logits rather than the loss because we have not provided labels.
        outputs = model(b_input_ids, token_type_ids=None,
                        attention_mask=b_input_mask, labels=b_labels)
    # Move logits and labels to CPU
    logits = outputs[1].detach().cpu().numpy()
    label_ids = b_labels.to('cpu').numpy()

    # Calculate the accuracy for this batch of test sentences.
    eval_loss += outputs[0].mean().item()
    predictions.extend([list(p) for p in np.argmax(logits, axis=2)])
    true_labels.extend(label_ids)
    batch_pred = [[list(p) for p in np.argmax(logits, axis=2)]]
    batch_true = [label_ids]


eval_loss = eval_loss / len(test_dataloader)
validation_loss_values.append(eval_loss)
print("Validation loss: {}".format(eval_loss))
pred_tags = [tag_values[p_i] for p, l in zip(predictions, true_labels)
                              for p_i, l_i in zip(p, l) if tag_values[l_i] != "PAD"]
test_tags = [tag_values[l_i] for l in true_labels
                              for l_i in l if tag_values[l_i] != "PAD"]
print("Precision: {}".format(precision_score(test_tags, pred_tags)))
f.write("\n\n Test set Classification Report:\n".format(precision_score(test_tags, pred_tags)))
print("Validation F1-Score: {}\n".format(f1_score(test_tags, pred_tags)))
report = classification_report(test_tags, pred_tags, output_dict=True)
reportDF = pd.DataFrame(report).transpose()
f.write(
  reportDF.to_string(header = True, index = False)
)
f.close()