import sklearn.metrics
import sys

y_true = [(int(e.strip())+1)/2 if e!='\n' else -1 for e in open(sys.argv[1],'r').readlines()]
y_score = [float(e.strip('\n')) for e in open(sys.argv[2],'r').readlines()]
y1 = [e for e in zip(y_true, y_score) if e[0] != -1]

print('{0} AUCROC {1}'.format(
               ' ',
                sklearn.metrics.roc_auc_score(
                  y_true = [e[0] for e in y1],
                  y_score = [e[1] for e in y1]
                ),
     ))
