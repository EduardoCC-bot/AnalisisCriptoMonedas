# Usamos una versión ligera de Python
FROM python:3.9-slim

# Creamos el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos el archivo de requerimientos e instalamos las librerías
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos tus scripts y la estructura de carpetas
COPY . .

# Comando por defecto: ejecutar la extracción y luego la transformación
CMD ["python", "-c", "import bronce; import transform_to_silver; bronce.extract_crypto_data(); transform_to_silver.process_to_silver()"]