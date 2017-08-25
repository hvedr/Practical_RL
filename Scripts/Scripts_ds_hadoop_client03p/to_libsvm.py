# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
i=0

def main():    
    for line in sys.stdin:
        if(i>5): break
        i+=1
        values = line.strip().split('\t')       
        print ' '.join([values[0]]+ [e + ':1' for e in values[1:]])

main()
