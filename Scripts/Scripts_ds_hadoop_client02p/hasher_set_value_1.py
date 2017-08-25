import mmh3
import sys
cnt=0
out = open(sys.argv[2],'w')

for r in open(sys.argv[1],'r'):
    try:
        l = r.split(' ')
        out.write(l[0] + ' ' + ' '.join('{}:1'.format(f.split(':')[0]) for f in l[1:]) + '\n')
    except:
        print('Error while hashing at {0} row: {1}'.format(cnt,r.replace('\n','')))
    cnt += 1
    if (cnt & (2 ** 19 - 1) == 0):
        print('In progress: ' + str(cnt))
        out.flush()

out.close()
