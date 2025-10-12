import time
def utc_now_ms(): return int(time.time() * 1000)
def to_ms(ts): t=int(float(ts)); return t if t>=10**12 else t*1000
def to_s(ts):  t=int(float(ts)); return t//1000 if t>=10**12 else t
