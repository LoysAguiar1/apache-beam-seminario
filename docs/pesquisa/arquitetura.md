# Arquitetura e Runners do Apache Beam

## 1. Introdução

A arquitetura do Apache Beam separa a **definição da lógica de processamento** da **infraestrutura responsável por executar o pipeline**.

No projeto, o desenvolvimento está sendo realizado com o Apache Beam SDK para Python, enquanto a execução inicial ocorre localmente.

## 2. Visão geral da arquitetura

```text
Desenvolvedor
     ↓
Beam SDK
Python / Java / Go
     ↓
Definição do Pipeline
Pipeline + PCollections + PTransforms
     ↓
Runner API / representação portátil
     ↓
Runner
     ↓
Ambiente de execução
```

## 3. SDKs

SDK significa **Software Development Kit**. Os SDKs fornecem as abstrações utilizadas para construir pipelines.

Entre os principais SDKs estudados estão Java, Python e Go. Neste projeto é utilizado o **Python SDK**.

```python
import apache_beam as beam

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Criar dados" >> beam.Create([1, 2, 3, 4])
        | "Dobrar" >> beam.Map(lambda valor: valor * 2)
    )
```

## 4. Definir não é executar

Ao escrever o código, o desenvolvedor define fontes, PCollections, PTransforms, regras e saídas.

```text
DEFINIÇÃO
Pipeline
├── entrada
├── PCollections
├── PTransforms
└── saída
```

Depois:

```text
EXECUÇÃO
Runner
   ↓
Infraestrutura
```

> O SDK descreve o pipeline; o Runner executa o pipeline.

## 5. Papel do Runner

Runner é o componente responsável por executar o pipeline em uma plataforma específica.

```text
Pipeline Beam
     ↓
Runner
     ├── DirectRunner
     ├── Flink Runner
     ├── Spark Runner
     └── Dataflow Runner
```

O Runner não define a regra de negócio. Ele executa a lógica já definida.

## 6. Runner API e portabilidade

A Runner API representa o pipeline de forma independente do SDK utilizado.

```text
Python SDK
Java SDK
Go SDK
    ↓
Runner API
    ↓
Runner
    ↓
Backend
```

Isso reduz o acoplamento entre linguagem e mecanismo de execução. A portabilidade não significa que todos os Runners oferecem exatamente os mesmos recursos.

## 7. DirectRunner

O DirectRunner é voltado principalmente para desenvolvimento, testes, aprendizado, depuração e pipelines pequenos.

```text
Pipeline Beam
     ↓
DirectRunner
     ↓
Computador local
```

Não é indicado como principal mecanismo para grandes cargas de produção.

### Relação com o projeto

Na Fase 1 foi validado um pipeline mínimo com:

```text
Create
  ↓
Filter
  ↓
CombinePerKey
  ↓
Map(print)
```

Os testes comprovaram criação de PCollections, execução de transforms, filtragem de registros inválidos, agregação correta por chave e execução local concluída.

Nos testes com Apache Beam 2.76.0, os logs apresentaram mensagens do PrismRunner durante a execução local. Esse comportamento pertence ao caminho interno de execução observado no ambiente e não altera o objetivo acadêmico da fase: validar a execução local de um pipeline Beam.

## 8. Flink Runner

O Flink Runner executa pipelines Beam sobre o Apache Flink.

```text
Pipeline Beam
     ↓
Flink Runner
     ↓
Apache Flink
```

É adequado a streaming, processamento distribuído, telemetria, sensores, logs, baixa latência e grandes volumes.

## 9. Spark Runner

O Spark Runner permite executar pipelines Beam no Apache Spark.

```text
Pipeline Beam
     ↓
Spark Runner
     ↓
Apache Spark
```

É útil principalmente quando a organização já utiliza o ecossistema Spark.

## 10. Dataflow Runner

O Dataflow Runner executa pipelines no Google Cloud Dataflow.

```text
Pipeline Beam
     ↓
Dataflow Runner
     ↓
Google Cloud Dataflow
     ↓
Workers gerenciados
```

Entre suas características estão infraestrutura gerenciada, execução distribuída, autoscaling, integração com Google Cloud e suporte a batch e streaming.

## 11. Comparação

| Runner | Ambiente | Distribuído | Batch | Streaming | Uso principal |
|---|---|---:|---:|---:|---|
| DirectRunner | Local | Não como cluster de produção | Sim | Desenvolvimento/testes | Desenvolvimento |
| Flink Runner | Apache Flink | Sim | Sim | Sim | Processamento distribuído |
| Spark Runner | Apache Spark | Sim | Sim | Depende da implementação | Ecossistema Spark |
| Dataflow Runner | Google Cloud | Sim | Sim | Sim | Produção gerenciada |

## 12. Arquitetura aplicada ao projeto

```text
Python 3.14.7
     ↓
Apache Beam SDK for Python
     ↓
Pipeline
     ├── PCollections
     └── PTransforms
     ↓
Execução local
     ↓
Resultado
```

A execução local foi escolhida por simplicidade, reprodutibilidade, facilidade de depuração, baixo risco e adequação ao nível introdutório.

## 13. O que muda ao trocar de Runner?

A lógica do pipeline pode ser portátil, mas cada Runner pode exigir configurações diferentes, dependências específicas, credenciais, infraestrutura, opções de execução e validação de compatibilidade.

Portabilidade reduz acoplamento, mas não elimina testes.

## 14. Resumo

```text
SDK         = construção da aplicação
Pipeline    = fluxo definido
PCollection = dados
PTransform  = operação
Runner      = execução
Runner API  = representação portátil
```

## 15. Conclusão

O Apache Beam separa a definição do processamento da execução. Os SDKs permitem construir pipelines, enquanto os Runners executam esses pipelines em diferentes plataformas.

No projeto, a execução local atende à fase de aprendizado e validação. Flink, Spark e Dataflow são estudados como alternativas para cenários distribuídos e de produção.

## 16. Referências

- APACHE BEAM. **Documentation**. https://beam.apache.org/documentation/
- APACHE BEAM. **Programming Guide**. https://beam.apache.org/documentation/programming-guide/
- APACHE BEAM. **Basics of the Beam model**. https://beam.apache.org/documentation/basics/
- APACHE BEAM. **Direct Runner**. https://beam.apache.org/documentation/runners/direct/
- APACHE BEAM. **Flink Runner**. https://beam.apache.org/documentation/runners/flink/
- APACHE BEAM. **Spark Runner**. https://beam.apache.org/documentation/runners/spark/
- APACHE BEAM. **Dataflow Runner**. https://beam.apache.org/documentation/runners/dataflow/
- APACHE BEAM. **Runner Authoring Guide**. https://beam.apache.org/contribute/runner-guide/
- APACHE BEAM. **Portability Framework Roadmap**. https://beam.apache.org/roadmap/portability/

Acessos registrados nos materiais do grupo em outubro de 2026.
