-- Base separada para los datos del pipeline (la base "airflow" guarda los metadatos)
CREATE DATABASE warehouse;
GRANT ALL PRIVILEGES ON DATABASE warehouse TO airflow;
