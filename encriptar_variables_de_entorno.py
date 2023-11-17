import subprocess
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from base64 import b64encode, b64decode

# Asegurarse de que la clave tenga una longitud de 24 bytes
def adjust_key_length(key):
    return key[:24].ljust(24, '\0')

# Clave para encriptación/desencriptación ajustada a longitud válida
encryption_key = adjust_key_length('oe2023')

# Función para cifrar las credenciales

def encrypt(value, key):
    key = adjust_key_length(key)
    data = value.encode()
    cipher = AES.new(key.encode(), AES.MODE_CBC)
    ct_bytes = cipher.encrypt(pad(data, AES.block_size))
    return b64encode(cipher.iv + ct_bytes).decode('utf-8')


# Definir las credenciales a encriptar
credentials = {
    'apiservicedefontana_database':"OFINANCE",
    'apiservicedefontana_user': "integrator",
    'apiservicedefontana_host': '192.168.149.20',
    'apiservicedefontana_password': "aZwY=d@tA79",
    'apiservicedefontana_port': '5432',  # Asumiendo que el puerto también debe ser encriptado
}

# Encriptar las credenciales y almacenarlas en variables de entorno del sistema
for cred_key, cred_value in credentials.items():
    encrypted_value = encrypt(cred_value, encryption_key)
    print(f'Longitud de {cred_key}: {len(encrypted_value)}')
    # Establecer las variables de entorno del sistema utilizando setx
    setx_command = ['setx', cred_key, encrypted_value, '/M']
    #El flag /M especifica que la variable de entorno se establece a nivel del sistema
    # para todos los usuarios. Sin /M, la variable se establece solo para el usuario actual.
    subprocess.run(setx_command, check=True)
    print(f'Variable de entorno del sistema {cred_key} establecida')
