import mmh3
import sys

salts = [213,21,54656]
visits_limit = 2000
fdim = 2 ** 20 # features space dimenension

def hasher(arg):
    inp = arg.split(' ') 
    row = {}
    for i in range(1,min(len(inp),visits_limit)):
        k,v = inp[i].split(':')
        for s in salts:
            h = mmh3.hash(k,s) % fdim
            if(h in row):
                row[h] = str(int(row[h]) + int(v))
            else:
                row[h] = v
    return(inp[0] + ' ' + ' '.join(['{0}:{1}'.format(k,row[k]) for k in sorted(row.keys())]))

print(hasher(' '.join(sys.argv[1:])))
#print('**'+' '.join(sys.argv[1:]) + '**')