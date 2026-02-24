import os
import pandas as pd
import random
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ===== CONEXÃO =====
usuario = "postgres"
senha = "loou"
host = "localhost"
porta = "5432"
banco = "projeto_onibus"

engine = create_engine(f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}')

# ===== CAMINHOS =====
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
caminho_raw = os.path.join(base_dir, "data", "raw")
caminho_processed = os.path.join(base_dir, "data", "processed")

os.makedirs(caminho_raw, exist_ok=True)
os.makedirs(caminho_processed, exist_ok=True)


# ===== TASK 0 — GERAR 20 CSV AUTOMÁTICOS =====
def task_0_gerar_csv():
    print("\nTask 0: Gerando 20 arquivos CSV de teste\n")

    for i in range(1, 21):
        data_base = datetime(2026, 2, 1) + timedelta(days=i)

        dados = []
        for _ in range(random.randint(8, 15)):
            registro = {
                "id_linha": random.randint(1, 5),
                "id_veiculo": random.randint(100, 110),
                "data_viagem": data_base.strftime("%Y-%m-%d"),
                "passageiros": random.randint(60, 220)
            }
            dados.append(registro)

        df = pd.DataFrame(dados)

        nome_arquivo = f"viagens_auto_{i:02d}.csv"
        df.to_csv(os.path.join(caminho_raw, nome_arquivo), index=False)

        print(f"{nome_arquivo} criado")

    print("\n20 arquivos criados com sucesso!\n")


# ===== TASK 1 — LEITURA =====
def task_1_leitura():
    print("Task 1: Buscando arquivos na pasta raw")
    arquivos = [f for f in os.listdir(caminho_raw) if f.endswith(".csv")]
    print(f"Arquivos encontrados: {len(arquivos)}")
    return arquivos


# ===== TASK 2 — ETL =====
def task_2_etl(arquivos):
    print("\nTask 2: Processando arquivos\n")

    for arquivo in arquivos:
        try:
            df = pd.read_csv(os.path.join(caminho_raw, arquivo))
            df.columns = df.columns.str.lower()
            df['data_viagem'] = pd.to_datetime(df['data_viagem'])
            df = df.dropna()

            linhas = len(df)

            df.to_sql("viagens", engine, if_exists="append", index=False)

            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO controle_carga (nome_arquivo, status, linhas_inseridas)
                    VALUES (:arquivo, 'SUCESSO', :linhas)
                """), {"arquivo": arquivo, "linhas": linhas})

            os.rename(
                os.path.join(caminho_raw, arquivo),
                os.path.join(caminho_processed, arquivo)
            )

            print(f"{arquivo} processado com sucesso!")

        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")


# ===== TASK 3 — ATUALIZAR DW =====
def task_3_atualiza_dw():
    print("\nTask 3: Atualizando Data Warehouse\n")

    with engine.connect() as conn:
        conn.execute(text("TRUNCATE fato_viagens;"))
        conn.execute(text("""
            INSERT INTO fato_viagens
            SELECT
                id_linha,
                data_viagem as data,
                SUM(passageiros)
            FROM viagens
            GROUP BY id_linha, data_viagem;
        """))

    print("DW atualizado com sucesso!\n")


# ===== TASK FINAL =====
def task_4_final():
    print("Pipeline finalizado com sucesso 🚀\n")


# ===== EXECUÇÃO DAG =====
def run_dag():
    task_0_gerar_csv()
    arquivos = task_1_leitura()
    if arquivos:
        task_2_etl(arquivos)
        task_3_atualiza_dw()
    task_4_final()


if __name__ == "__main__":
    run_dag()