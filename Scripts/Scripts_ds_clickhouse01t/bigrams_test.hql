select concat(label,' ', features_str) as v
from user_kposminin.urlfr_w_ref_phone_20161019_2
where ymd = '2016-10-19' and has_phone = 1
