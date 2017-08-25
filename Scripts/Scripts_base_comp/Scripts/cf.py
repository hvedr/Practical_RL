'''
(C) Tinkoff bank 2016
Утилита преобразования исходных логов LiveInternet в формат, подходящий
для загрузки их в ClickHouse.
ВМЕСТО URL ВЫГРУЖАЮТСЯ фрагменты URL!

time  pigz -d -c 2016-06-26__23.50.19.gz | pypy li2ch_url_fr_2fields.py >/dev/null

real    0m3.416s
user    0m3.284s
sys     0m0.120s

Выходной формат (разделитель - tab):
YYYY-MM-DD TimeUTS CookieId IpUINT32 URLfragment
'''

import sys
import datetime

for line in sys.stdin:
    uts, uid, ip, url = line.strip().split('\t')[:4]
    ip = ip.split('.')
    ip = int(ip[3]) + 256*(int(ip[2]) + 256*(int(ip[1]) + 256*int(ip[0])))
    date = datetime.datetime.fromtimestamp(int(uts)).strftime('%Y-%m-%d')
    url_fr = url.split('?', 1)[0].replace('\\', '\\\\').split('/', 5)
    if len(url_fr[0]) == 0:
        continue
    #uid = uid[:16]
    ds = '%s\t%s\t%s\t%d\t%s'%(date,uts,uid, ip, url_fr[0])
    print(ds)
    #print('0x%s\t%d\t%s'%(uid, ip, domain))
    #print('0x%s\t%s'%(uid, domain))
    for i, s in enumerate(url_fr[1:4]):
        if len(s) > 0:
            #print('0x%s\t%d\t%s[%d]%s'%(uid, ip, domain, i, s))
            #print('0x%s\t%s[%d]%s'%(uid, domain, i, s))
            print('%s[%d]%s'%(ds, i, s))
