CREATE TABLE IF NOT EXISTS viagens (
    id SERIAL PRIMARY KEY,
    data_viagem DATE,
    linha VARCHAR(50),
    origem VARCHAR(100),
    destino VARCHAR(100),
    qtd_passageiros INT,
    valor_total NUMERIC(10,2)
);