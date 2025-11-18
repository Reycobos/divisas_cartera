import requests, hashlib, hmac, time, random, string, json

# 1. Definir credenciales (¡Reemplaza estos valores por los tuyos!)
LBANK_API_KEY = "TU_API_KEY_AQUI"
LBANK_SECRET_KEY = "TU_SECRET_KEY_AQUI"

# 2. Endpoint público: obtener todos los pares disponibles
pairs_url = "https://api.lbkex.com/v2/currencyPairs.do"
resp = requests.get(pairs_url)
if resp.status_code == 200:
    data = resp.json()
    # Algunos endpoints públicos de LBank devuelven directamente la lista, otros envuelven en 'data'
    pairs_list = data.get("data", data)  
    print(f"Pares disponibles ({len(pairs_list)} pares):")
    for pair in pairs_list:
        print(" -", pair)
else:
    print(f"Error al obtener pares. Código HTTP {resp.status_code}")
    exit(1)

# 3. Preparar parámetros de autenticación para endpoint privado
timestamp = str(int(time.time() * 1000))  # milisegundos actuales
echostr = ''.join(random.choices(string.ascii_letters + string.digits, k=32))
signature_method = "HmacSHA256"

# Construir diccionario de parámetros requeridos (sin 'sign')
params = {
    "api_key": LBANK_API_KEY,
    "timestamp": timestamp,
    "echostr": echostr,
    "signature_method": signature_method
}
# Ordenar parámetros alfabéticamente y concatenar "key=value"
param_str = '&'.join(f"{k}={params[k]}" for k in sorted(params))
# Calcular MD5 en hexadecimal (mayúsculas)
md5_digest = hashlib.md5(param_str.encode('utf-8')).hexdigest().upper()
# Calcular firma HmacSHA256 del MD5 usando la Secret Key
sign = hmac.new(LBANK_SECRET_KEY.encode('utf-8'), md5_digest.encode('utf-8'), hashlib.sha256).hexdigest()
params["sign"] = sign  # agregar la firma a los parámetros

# 4. Endpoint privado: obtener información de cuenta (requiere autenticación)
private_url = "https://api.lbkex.com/v2/supplement/user_info.do"
resp_priv = requests.post(private_url, data=params)
if resp_priv.status_code == 200:
    result = resp_priv.json()
    # Imprimir la respuesta formateada (JSON prettified)
    print("\nRespuesta de /supplement/user_info.do:")
    print(json.dumps(result, indent=2))
else:
    print(f"Error en petición privada. Código HTTP {resp_priv.status_code}")