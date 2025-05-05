# Use official Python base image (more suitable than Azure Functions image for Streamlit apps)
FROM python:3.12-slim

# Install required system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        unixodbc-dev \
        curl \
        gnupg \
        libpq-dev \
        && rm -rf /var/lib/apt/lists/*

# Optional: Install Microsoft ODBC Driver for SQL Server if needed
# RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
#     curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
#     apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Set working directory
WORKDIR /app

# Copy your application files
COPY . /app

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Streamlit default port is 8501
EXPOSE 8501

# Command to run Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.enableCORS=false"]
