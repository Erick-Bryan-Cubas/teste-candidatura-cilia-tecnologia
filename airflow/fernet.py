from cryptography.fernet import Fernet

# Gera e imprime a chave Fernet
print(Fernet.generate_key().decode())
