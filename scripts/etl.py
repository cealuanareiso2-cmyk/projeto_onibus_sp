import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# 🔹 Conexão com PostgreSQL
usuario = "postgres"
senha = "loou"
host = "localhost"
porta = "5432"
banco = "projeto_onibus"

engine = create_engine(f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}')

# 🔹 Criando dados simulados (por enquanto)
dados = {
    "id_linha": [1, 2, 1],
    "id_veiculo": [101, 102, 103],
    "data_viagem": ["2026-02-01", "2026-02-01", "2026-02-02"],
    "passageiros": [120, 80, 150]
}

df = pd.DataFrame(dados)

df['data_viagem'] = pd.to_datetime(df['data_viagem'])

# 🔹 Inserindo no banco
df.to_sql("viagens", engine, if_exists="replace", index=False)

print("Dados inseridos com sucesso!")