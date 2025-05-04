README_CONTENT = '''
# SaludDigna – Plataforma de Analisis Temporal de Datos

SaludDigna es una aplicacion interactiva construida con Python, Streamlit y Azure para realizar analisis de datos temporales desde bases de datos SQL. Incluye conexion segura con Azure Key Vault, procesamiento automatizado y despliegue continuo mediante CI/CD con GitHub Actions y Azure Virtual Machines.

---

## Que hace esta app?

- Se conecta a una base de datos en Azure SQL Server
- Utiliza Azure Key Vault para proteger tus credenciales
- Realiza analisis exploratorios (EDA) automáticos
- Detecta y visualiza outliers temporales
- Aplica limpieza de datos y genera reportes finales
- Despliegue en la nube con CI/CD

---

## Estructura del Proyecto

SaludDigna/
├── .env                    # Variables sensibles
├── app.py                 # Interfaz Streamlit
├── deploy.yml             # GitHub Actions
├── requirements.txt       # Dependencias
├── SecretKeys.py          # Conexión a Key Vault
├── DatabaseEDA.py         # Analisis SQL
├── utils/
│   └── upload_to_sql.py   # Subida de datos
├── TemporalCleaner.py
├── TemporalDataLoader.py
├── TemporalOutlierVisualizer.py
├── TemporalReport.py
├── DescriptionsEmbeddings.py
└── README.md

---

## Requisitos

- Python 3.8 o superior
- Azure SQL Database
- Azure Key Vault
- Azure VM o cualquier entorno con Python
- Cuenta GitHub

---

## Instalacion local paso a paso

1. Clona el repositorio:

    git clone https://github.com/emilianoGGauna/SaludDigna.git
    cd SaludDigna

2. Crea un entorno virtual:

    python -m venv .venv
    .venv\\Scripts\\activate

3. Instala dependencias:

    pip install -r requirements.txt

4. Crea el archivo .env con tus secretos:

    KEY_VAULT_URL=https://sdignakeys.vault.azure.net/
    AZURE_CLIENT_ID=xxxx
    AZURE_CLIENT_SECRET=xxxx
    TENANT_ID=xxxx

---

## Ejecutar la aplicacion

    streamlit run app.py

---

## Despliegue CI/CD con GitHub Actions y Azure VM

1. Sube tu proyecto a GitHub:

    git init
    git remote add origin https://github.com/TU_USUARIO/SaludDigna.git
    git add .
    git commit -m "Deploy inicial"
    git push -u origin main

2. Crea un archivo llamado `.github/workflows/deploy.yml` con tu estrategia CI/CD

---

## Seguridad

El proyecto no expone contraseñas. Todas las credenciales se obtienen desde Azure Key Vault usando DefaultAzureCredential con un Service Principal.

---
