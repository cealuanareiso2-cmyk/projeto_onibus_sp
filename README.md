# 🚍 Projeto de Engenharia de Dados – Transporte Público SP

## 📌 Sobre o Projeto
Este projeto simula um pipeline completo de Engenharia de Dados utilizando dados de transporte público (ônibus) da cidade de São Paulo.

O objetivo foi construir um fluxo completo desde a modelagem relacional até a criação de um Data Warehouse no modelo estrela para análises estratégicas.

---

## 🏗 Arquitetura do Projeto

1. Modelagem relacional no PostgreSQL
2. Criação de tabelas dimensionais (modelo estrela)
3. Construção de tabela fato
4. Preparação para consumo analítico (BI)

---

## 🛠 Tecnologias Utilizadas

- PostgreSQL
- SQL
- Python
- Power BI
- Git & GitHub

---

## ⭐ Modelagem Dimensional (Star Schema)

Tabelas criadas:

- dim_linha
- dim_tempo
- fato_viagens

Modelo focado em análise de:
- Volume de viagens
- Distribuição por região
- Análise temporal (ano, mês, dia)

---

## 📊 Objetivo Analítico

Permitir análises como:

- Qual região possui maior volume de viagens?
- Qual mês possui maior demanda?
- Evolução anual de utilização do transporte?

---

## 🚀 Próximos Passos

- Automatização do pipeline com Python
- Deploy em ambiente cloud (GCP ou AWS)
- Dashboard interativo no Power BI

---

## 👩‍💻 Autora

Luana Reis  
Projeto desenvolvido com foco em transição para Engenharia de Dados.
