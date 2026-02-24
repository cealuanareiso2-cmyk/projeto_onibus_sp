-- Dimensão Tempo
CREATE TABLE dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    data DATE,
    ano INT,
    mes INT,
    dia INT
);

-- Dimensão Linha
CREATE TABLE dim_linha (
    id_linha SERIAL PRIMARY KEY,
    nome_linha VARCHAR(50)
);

-- Fato Viagens
CREATE TABLE fato_viagens (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INT REFERENCES dim_tempo(id_tempo),
    id_linha INT REFERENCES dim_linha(id_linha),
    qtd_passageiros INT,
    valor_total NUMERIC(10,2)
);