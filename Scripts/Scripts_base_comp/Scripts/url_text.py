# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def main():    
    for line in sys.stdin:
        values = line.strip().split('\t')
       
        print ' '.join([values[0]] + [e + ':1' for e in values[1:]])

main()