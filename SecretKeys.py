#!/usr/bin/env python3
"""
secret_keys.py

Clase simple para leer secretos de Azure Key Vault.
Requiere las variables de entorno:
  KEY_VAULT_URL, AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
"""

import os
import logging
from functools import lru_cache

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import AzureError
from dotenv import load_dotenv


class SecretKeys:
    """Helper Class to retrieve secrets from Azure Key Vault"""

    def __init__(self) -> None:
        load_dotenv()                              # Lee el .env si existe
        self._vault_url = os.getenv("KEY_VAULT_URL")
        if not self._vault_url:
            raise ValueError("La variable KEY_VAULT_URL no está definida")

        self._credential = DefaultAzureCredential()
        self._client = SecretClient(
            vault_url=self._vault_url,
            credential=self._credential
        )
        self._verify_credentials()

    # ---------- Métodos privados ----------

    def _verify_credentials(self) -> None:
        """Comprueba que podamos obtener un token"""
        try:
            self._credential.get_token("https://vault.azure.net/.default")
            logging.info("Credenciales de Azure verificadas correctamente.")
        except AzureError as err:
            logging.warning(
                "No se pudieron verificar las credenciales de Azure: %s", err
            )

    # ---------- API pública ----------

    @lru_cache(maxsize=32)
    def get(self, secret_name: str) -> str:
        """Devuelve el valor de un secreto"""
        try:
            return self._client.get_secret(secret_name).value
        except AzureError as err:
            logging.error("Error al leer el secreto '%s': %s", secret_name, err)
            raise

    # ---------- Utilidades estáticas ----------

    @staticmethod
    def configure_logging(level: int = logging.INFO) -> None:
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

# ---------------- Ejemplo standalone ----------------
if __name__ == "__main__":
    SecretKeys.configure_logging()

    sk = SecretKeys()
    secret_name = "SERVER"        # nombre exacto del secreto en tu Key Vault
    secret_value = sk.get(secret_name)
    print(f"Valor de '{secret_name}':", secret_value)
