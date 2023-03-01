import random
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_validate
from sklearn.metrics import precision_score, recall_score, f1_score


with open('part-r-00000', 'r') as f:
    lines = f.readlines()

lines = [line.replace('\t', ',') for line in lines] # replace tabs with ','
lines = [line[:-2] for line in lines] # remove last character of each line
vectors = []
labels = []
nounPairs = []

for line in lines:
    parts = line.strip().split(',')
    label = parts[3]
    w1 = parts[0]
    w2 = parts[1]
    nounPairs.append((w1,w2))
    vector = np.array([float(val) for val in parts[4:]])
    vectors.append(vector)
    labels.append(label)
    

# convert labels to binary (1 if true, 0 if false)
binary_labels = [1 if label == 'true' else 0 for label in labels]


classifier = RandomForestClassifier()
scores = cross_validate(classifier, vectors, binary_labels, cv=10)
classifier.fit(vectors, binary_labels)
predicted_labels = classifier.predict(vectors)

print("Precision-", precision_score(binary_labels, predicted_labels))
print("Recall-", recall_score(binary_labels, predicted_labels))
print("F1-", f1_score(binary_labels, predicted_labels))

with open('Noun-Pairs.txt', 'w') as f:
    size = len(nounPairs)
    for word in nounPairs:
        size -= 1
        f.write(word[0] + ',' + word[1])
        if size > 0:
            f.write('\n')
            
fp, fn, tp, tn = [], [], [], []
for i in range(len(predicted_labels)):
    
    if random.randint(1, 20) == 10: #randomly select pairs to have different outputs each time
        if predicted_labels[i] == 1 and binary_labels[i] == 1 and len(tp) <= 5:
            tp.append(nounPairs[i])
        elif predicted_labels[i] == 1 and binary_labels[i] == 0 and len(fp) <= 5:
            fp.append(nounPairs[i])
        elif predicted_labels[i] == 0 and binary_labels[i] == 1 and len(fn) <= 5:
            fn.append(nounPairs[i])
        elif predicted_labels[i] == 0 and binary_labels[i] == 0 and len(tn) <= 5:
            tn.append(nounPairs[i])
        if len(tp) == 5 and len(tn) == 5 and len(fp) == 5 and len(fn) == 5:
            break
    
print("TP: ", tp)
print("FP: ", fp)
print("FN: ", fn)
print("TN: ", tn)