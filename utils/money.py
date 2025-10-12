from decimal import Decimal, getcontext, ROUND_HALF_EVEN
getcontext().prec = 28
D = lambda x: Decimal(str(0 if x in (None, "",) else x))
def usd(x): return D(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
def quant(x, places=6):
    q = Decimal(10) ** -places
    return D(x).quantize(q, rounding=ROUND_HALF_EVEN)
def normalize_fee(x): return -abs(D(x))
def to_float(x): return float(D(x))


