from dotenv import load_dotenv
import os

load_dotenv()
print("URL        :", os.getenv("KEY_VAULT_URL"))
print("Tenant     :", os.getenv("AZURE_TENANT_ID"))
print("Client     :", os.getenv("AZURE_CLIENT_ID"))
print("ClientSecret:", os.getenv("AZURE_CLIENT_SECRET"))
