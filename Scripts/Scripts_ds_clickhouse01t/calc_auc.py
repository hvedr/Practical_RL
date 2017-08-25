import sklearn.metrics
import sys


print('{0} AUCROC {1}'.format(
               '',
                sklearn.metrics.roc_auc_score(
                  y_true = [int(e) for e in open(sys.argv[1],'r').read()],
                  y_score = [float(e.strip('\n')) for e in open(sys.argv[2],'r').readlines()]
                ),
                                              
))
