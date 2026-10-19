# Modelo de programação — Pipeline, PCollection e PTransform

## 1. Introdução

Um pipeline Apache Beam pode ser entendido como um fluxo de processamento organizado como um grafo de operações.

Os três conceitos fundamentais desta estrutura são:

- `Pipeline`;
- `PCollection`;
- `PTransform`.

```text
Fonte
  ↓
PCollection
  ↓
PTransform
  ↓
Nova PCollection
  ↓
PTransform
  ↓
Saída
```

## 2. Pipeline

O `Pipeline` representa a tarefa completa de processamento. Ele organiza leitura ou criação dos dados, transformações, agregações, saídas e opções de execução.

```text
CSV de vendas
     ↓
Leitura
     ↓
Filtragem
     ↓
Agrupamento
     ↓
Soma
     ↓
Resultado
```

O pipeline primeiro define o grafo de processamento. Depois, um Runner executa essa definição.

## 3. PCollection

A `PCollection` representa os dados que circulam pelo pipeline. Ela pode conter textos, números, registros, vendas, eventos, mensagens ou resultados de transformações anteriores.

Uma característica importante é que a PCollection é tratada como imutável: uma transformação não altera diretamente a coleção original, mas produz uma nova PCollection.

```text
PCollection original
10 → 20 → 30 → 40

        ↓ Filter

Nova PCollection
20 → 40
```

## 4. PTransform

`PTransform` representa uma operação aplicada aos dados.

Exemplos:

- `Create`;
- `Map`;
- `Filter`;
- `ParDo`;
- `GroupByKey`;
- `CombinePerKey`;
- operações de leitura e escrita.

```python
pipeline
| "Criar números" >> beam.Create([1, 2, 3, 4, 5, 6])
| "Filtrar pares" >> beam.Filter(lambda n: n % 2 == 0)
| "Mostrar" >> beam.Map(print)
```

Nesse fluxo, `Create` produz uma PCollection, `Filter` produz uma nova PCollection e `Map` executa outra transformação.

## 5. Relação entre os conceitos

- o **Pipeline** organiza o processamento;
- as **PCollections** representam os dados;
- os **PTransforms** representam as operações.

```text
Pipeline
│
├── PCollection
│      ↓
├── PTransform
│      ↓
├── PCollection
│      ↓
└── PTransform
```

## 6. Fluxo básico de um pipeline Beam

1. Criar o objeto `Pipeline`.
2. Ler ou criar os dados iniciais.
3. Obter uma `PCollection`.
4. Aplicar um ou mais `PTransforms`.
5. Gerar novas `PCollections`.
6. Definir a saída.
7. Executar o pipeline com um Runner.

## 7. Dados bounded e unbounded

| Tipo | Característica | Exemplo | Uso comum |
|---|---|---|---|
| Bounded | Conjunto finito | CSV, tabela, lista | Batch |
| Unbounded | Fluxo contínuo | Eventos, sensores, mensagens | Streaming |

### Bounded

```text
Arquivo CSV
    ↓
PCollection finita
    ↓
Processamento batch
```

### Unbounded

```text
Eventos
   ↓
PCollection contínua
   ↓
Processamento streaming
```

## 8. Exemplo simples em Python

```python
import apache_beam as beam

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Criar dados" >> beam.Create(["casa", "carro", "escola"])
        | "Converter para maiúsculas" >> beam.Map(str.upper)
        | "Mostrar resultado" >> beam.Map(print)
    )
```

## 9. Exemplo com filtro

```python
import apache_beam as beam

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Criar números" >> beam.Create([1, 2, 3, 4, 5, 6])
        | "Filtrar pares" >> beam.Filter(lambda n: n % 2 == 0)
        | "Mostrar pares" >> beam.Map(print)
    )
```

Resultado esperado:

```text
2
4
6
```

## 10. Conclusão

O modelo de programação do Apache Beam organiza o processamento por meio de um Pipeline, no qual os dados são representados por PCollections e as operações por PTransforms. Esses conceitos são utilizados tanto em batch quanto em streaming.

## 11. Referências

- APACHE SOFTWARE FOUNDATION. **Beam Quickstart for Python**. https://beam.apache.org/get-started/quickstart/python/
- APACHE SOFTWARE FOUNDATION. **Basics of the Beam Model**. https://beam.apache.org/documentation/basics/
- APACHE SOFTWARE FOUNDATION. **Apache Beam Programming Guide**. https://beam.apache.org/documentation/programming-guide/
- APACHE SOFTWARE FOUNDATION. **Apache Beam Glossary**. https://beam.apache.org/documentation/glossary/
- APACHE SOFTWARE FOUNDATION. **Test your pipeline**. https://beam.apache.org/documentation/pipelines/test-your-pipeline/

Acessos registrados nos materiais do grupo em outubro de 2026.
