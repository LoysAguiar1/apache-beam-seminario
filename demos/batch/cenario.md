# Demonstração Batch — Dataset e cenário de negócio

## 1. Objetivo

A demonstração batch utilizará um cenário de vendas de uma empresa de produtos de tecnologia. O objetivo é mostrar, de forma simples e reproduzível, como o Apache Beam pode ser utilizado em um fluxo semelhante a um processo ETL.

O pipeline deverá ler um arquivo CSV com vendas já registradas, validar os registros, calcular o faturamento de cada venda e gerar o faturamento total por categoria.

## 2. Problema de negócio

Uma empresa comercializa produtos de tecnologia e recebe registros diários de vendas. Para apoiar a análise gerencial, a empresa precisa consolidar essas informações e responder à seguinte pergunta:

> Qual foi o faturamento total de cada categoria de produtos no período analisado?

Os dados de origem podem conter registros inconsistentes. Por isso, antes da agregação, é necessário validar os registros e impedir que valores inválidos afetem os indicadores.

## 3. Fonte de dados

O arquivo de entrada é:

```text
data/vendas.csv
```

Estrutura:

| Campo | Tipo esperado | Descrição |
|---|---|---|
| `data` | data no formato AAAA-MM-DD | Data da venda |
| `produto` | texto | Nome do produto |
| `categoria` | texto | Categoria do produto |
| `quantidade` | inteiro | Quantidade vendida |
| `preco` | decimal | Preço unitário |

## 4. Categorias utilizadas

O dataset utiliza três categorias:

- `Eletronicos`
- `Perifericos`
- `Acessorios`

A quantidade reduzida de categorias facilita a visualização dos resultados durante a apresentação.

## 5. Registros inválidos incluídos propositalmente

O arquivo contém três registros inválidos para permitir que a etapa de validação do pipeline seja demonstrada:

```text
Cabo USB       → quantidade = 0
Webcam         → preço = -250.00
produto vazio  → campo obrigatório ausente
```

Esses registros não devem participar dos cálculos finais.

## 6. Regras de validação previstas

Um registro será considerado válido quando:

1. `data` estiver preenchida;
2. `produto` estiver preenchido;
3. `categoria` estiver preenchida;
4. `quantidade` for maior que zero;
5. `preco` for maior que zero.

A implementação dessas regras pertence à etapa de leitura, transformação e validação da Fase 3.

## 7. Transformação principal

Para cada venda válida:

```text
faturamento_da_venda = quantidade × preco
```

Exemplo:

```text
Notebook
quantidade = 2
preço = 3500.00

2 × 3500.00 = 7000.00
```

Depois, os valores serão agrupados por categoria:

```text
Venda válida
     ↓
Calcular faturamento
     ↓
(categoria, faturamento)
     ↓
CombinePerKey(sum)
     ↓
Faturamento por categoria
```

## 8. Resultado de referência

Considerando apenas os 15 registros válidos, o resultado esperado é:

```text
Eletronicos → R$ 17.400,00
Perifericos → R$ 3.150,00
Acessorios  → R$ 1.620,00
```

Faturamento total válido:

```text
R$ 22.170,00
```

Quantidade total de unidades válidas:

```text
46 unidades
```

Esses valores servem como referência para testar as próximas tarefas da Fase 3.

## 9. Por que este dataset foi escolhido?

O dataset foi mantido pequeno de propósito para que:

- o resultado possa ser conferido manualmente;
- a execução seja rápida;
- os conceitos do Beam sejam mais importantes que o volume de dados;
- seja possível demonstrar leitura, limpeza, transformação e agregação;
- erros de entrada possam ser mostrados de forma controlada;
- a apresentação não dependa de banco de dados ou serviço externo.

Mesmo sendo pequeno, o fluxo representa um processo de dados real que poderia ser aplicado a arquivos muito maiores em outro ambiente de execução.

## 10. Relação com ETL

O cenário permite demonstrar claramente as etapas:

```text
EXTRACT
Ler vendas.csv

    ↓

TRANSFORM
Converter tipos
Validar registros
Calcular faturamento
Agrupar por categoria

    ↓

LOAD / OUTPUT
Gerar resultado consolidado
```

Isso conecta diretamente a demonstração com a disciplina de Projeto de Análise e Fluxo de Dados — OLAP e ETL.

## 11. Entregáveis desta tarefa

- `data/vendas.csv`
- `demos/batch/cenario.md`

Com esses dois artefatos, a próxima tarefa já pode implementar a leitura, o parse e a validação dos dados.
