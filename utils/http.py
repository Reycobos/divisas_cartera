import requests, time as _t
def get_session(timeout=15):
    s = requests.Session()
    s.request = _wrap(s.request, timeout=timeout)
    return s

def _wrap(func, timeout=15, retries=3, backoff=0.5):
    def _req(method, url, **kw):
        kw.setdefault("timeout", timeout)
        for i in range(retries):
            try:
                return func(method, url, **kw)
            except Exception:
                if i == retries-1: raise
                _t.sleep(backoff * (2**i))
    return _req

