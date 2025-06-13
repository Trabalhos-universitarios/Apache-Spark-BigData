# 💼 Análise de Transações Comerciais com Apache Spark

## 📘 Contexto do Projeto

Você foi contratado para integrar a equipe de análise de dados de uma grande empresa multinacional. Essa equipe utiliza o framework **Apache Spark** com a linguagem **Python** para processar grandes volumes de dados.

Sua missão é utilizar o Spark para extrair informações de um conjunto de **transações comerciais internacionais** realizadas nos últimos **30 anos**, baseado em um dataset com mais de **8 milhões de registros**.

---

## 📊 Estrutura do Dataset

O arquivo está em **formato CSV** e separado por `;`, com as seguintes **10 colunas**:

| Coluna           | Descrição                                               |
|------------------|----------------------------------------------------------|
| `Country`        | País envolvido na transação comercial                   |
| `Year`           | Ano da transação                                        |
| `Commodity code` | Código do item transacionado                            |
| `Commodity`      | Descrição do item transacionado                         |
| `Flow`           | Tipo da transação (Exemplo: Export ou Import)           |
| `Price`          | Preço em dólares (USD)                                  |
| `Weight`         | Peso da mercadoria                                      |
| `Unit`           | Unidade de medida do item                               |
| `Amount`         | Quantidade transacionada                                |
| `Category`       | Categoria da mercadoria (Exemplo: Animais vivos)        |

---

## ✅ Requisitos Gerais

Para todas as tarefas abaixo, é obrigatório:

1. **Remover o cabeçalho** do arquivo CSV.
2. **Tratar dados faltantes ou inconsistentes**.
3. **Fornecer**:
   - O **código-fonte** com PySpark (`.py`)
   - O **resultado da execução** salvo em `.txt`, um para cada item.

> ⚠️ A submissão deve ser completa. Arquivos parciais resultarão em pontuação parcial.

---

## 🧠 Tarefas a serem implementadas

Cada item abaixo deve ser resolvido com **Apache Spark usando Python**. Utilize RDDs e PairRDDs conforme solicitado.

### 🔹 1. Transações com o Brasil (RDD) – 0,5 ponto
Retornar o **número total** de transações onde o país é **Brazil**, utilizando **RDD**.  
📤 Resultado: valor único.

---

### 🔹 2. Transações com o Brasil (PairRDD) – 1,0 ponto  
Retornar o número total de transações onde o país é **Brazil**, utilizando **PairRDD**.  
📤 Resultado: chave (Brazil), valor (quantidade).

---

### 🔹 3. Transações do Brasil em 2016 (RDD) – 0,5 ponto  
Retornar o total de transações com país **Brazil** e ano **2016**, usando **RDD**.  
📤 Resultado: valor único.

---

### 🔹 4. Transações do Brasil em 2016 (PairRDD) – 1,0 ponto  
Mesmo filtro anterior, agora com **PairRDD**.  
📤 Resultado: chave (Brazil, 2016), valor (quantidade).

---

### 🔹 5. Transações por Flow e Ano (desde 2010) – 1,0 ponto  
Contar transações agrupadas por `(ano, flow)` a partir de **2010**, transformando `Flow` em **maiúsculo**.  
📤 Resultado: chave (ano, FLOW), valor (quantidade), ordenado por ano.

---

### 🔹 6. Média de `Price` em 2016 (RDD) – 0,5 ponto  
Retornar apenas o valor da **média** da coluna `Price` para o ano de **2016**, usando **RDD**.  
📤 Resultado: valor único.

---

### 🔹 7. Média de `Price` em 2016 (PairRDD) – 1,0 ponto  
Mesma média da questão anterior, agora usando **PairRDD**.  
📤 Resultado: chave (2016), valor (média).

---

### 🔹 8. Máximo e Mínimo de `Price` por Categoria e Ano – 1,0 ponto  
Usar PairRDD para retornar o **máximo e mínimo** da coluna `Price` agrupados por `(ano, categoria)`.  
A coluna `Category` deve estar em **maiúsculo** e a saída ordenada por **país**.  
📤 Resultado: chave (ano, CATEGORIA), valor (max, min)

---

### 🔹 9. País com maior exportação (PairRDD) – 1,5 ponto  
Identificar o **país com maior número de transações do tipo Export**.  
📤 Resultado: chave (país, Export), valor (quantidade).

---

### 🔹 10. Menor preço por país e ano – 1,0 ponto  
Retornar o **menor preço** agrupado por `(país, ano)`, ordenado por **ano (crescente)** e **país (alfabético)**.  
📤 Resultado: chave (ano, país), valor (menor preço)

---

### 🔹 11. Maior preço por kg em exportações – 1,0 ponto  
Localizar a transação de tipo **Export** com **maior valor por kg** (`Price / Weight`) e retornar:
- **Ano**
- **País**
- **Categoria**

📤 Resultado: valor único com os 3 campos informados.

---

## 📁 Organização esperada da entrega

---

![img.png](assets/readme/logo_pucpr.png)