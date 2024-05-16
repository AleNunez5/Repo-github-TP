# Establece la imagen base de Python 3.10 Slim (Bullseye)
FROM python:3.10-slim-bullseye

# Establece el directorio de trabajo en /app
WORKDIR /app

# Copia el archivo requirements.txt en el directorio de trabajo
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el directorio de origen de donde copiar los archivos.
COPY . .

# Expone el puerto 80 en el contenedor
EXPOSE 80

# Define el comando por defecto para ejecutar la aplicación
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
