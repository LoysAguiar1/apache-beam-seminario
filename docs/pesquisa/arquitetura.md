# Arquitetura e Runners do Apache Beam

## 1. Introdução

O Apache Beam é um modelo de programação para construção de pipelines de processamento de dados. Uma de suas principais propostas é separar **a definição da lógica do pipeline** da **infraestrutura responsável por executá-lo**.

Essa separação permite que o desenvolvedor descreva o processamento utilizando as abstrações do Beam — como `Pipeline`, `PCollection` e `PTransform` — e escolha um **Runner** para executar esse pipeline em um ambiente específico.

Neste projeto, a aplicação está sendo desenvolvida com o **Apache Beam SDK para Python**. Durante a fase inicial, a execução é realizada localmente, adequada para desenvolvimento e validação. Em cenários de maior escala, o mesmo modelo de programação pode ser executado por Runners associados a mecanismos distribuídos, como Apache Flink, Apache Spark e Google Cloud Dataflow.

Este documento apresenta os principais elementos da arquitetura do Apache Beam:

- Beam SDKs;
- definição e execução de um pipeline;
- papel dos Runners;
- Runner API e portabilidade;
- DirectRunner;
- Flink Runner;
- Spark Runner;
- Dataflow Runner;
- comparação entre os Runners;
- aplicação da arquitetura no projeto do grupo.

---

## 2. Visão geral da arquitetura

Uma forma simplificada de representar a arquitetura do Apache Beam é:

```text
Desenvolvedor
     |
     v
Apache Beam SDK
Python / Java / Go
     |
     v
Definição do Pipeline
Pipeline + PCollections + PTransforms
     |
     v
Representação portátil / Runner API
     |
     v
Runner
     |
     +----------------+----------------+----------------+
     |                |                |                |
     v                v                v                v
DirectRunner     Flink Runner     Spark Runner     Dataflow Runner
     |                |                |                |
     v                v                v                v
Execução local   Apache Flink     Apache Spark     Google Cloud
```

O ponto central é que o programa Beam descreve **o que deve ser processado**, enquanto o Runner é responsável por decidir **como e onde o pipeline será executado**.

---

## 3. Beam SDKs

### 3.1 O que é um SDK?

SDK significa **Software Development Kit**. No Apache Beam, os SDKs fornecem as classes, funções e abstrações utilizadas pelo desenvolvedor para construir pipelines.

Entre os SDKs oficiais mais conhecidos estão:

- Java SDK;
- Python SDK;
- Go SDK.

A documentação do Apache Beam também apresenta outras interfaces em desenvolvimento ou com escopo específico, mas neste projeto o foco é o **Python SDK**, utilizado na implementação prática.

### 3.2 Papel do SDK

O SDK permite escrever a lógica do pipeline utilizando conceitos do modelo Beam.

Por exemplo:

```python
import apache_beam as beam

with beam.Pipeline() as pipeline:
    resultado = (
        pipeline
        | "Criar dados" >> beam.Create([1, 2, 3, 4])
        | "Dobrar valores" >> beam.Map(lambda valor: valor * 2)
    )
```

Nesse exemplo:

- `beam.Pipeline()` cria a estrutura do pipeline;
- `beam.Create()` cria uma `PCollection`;
- `beam.Map()` representa um `PTransform`;
- o código descreve o processamento que deverá ser realizado.

O SDK, portanto, é utilizado principalmente para **definir a aplicação de processamento**.

---

## 4. Definir um pipeline não é o mesmo que executá-lo

Essa diferença é essencial para compreender a arquitetura do Apache Beam.

### 4.1 Definição

Ao escrever um programa Beam, o desenvolvedor define:

- fontes de dados;
- `PCollections`;
- `PTransforms`;
- regras de processamento;
- saídas;
- opções de execução.

Exemplo conceitual:

```text
Entrada
  |
  v
PCollection
  |
  v
Filter
  |
  v
PCollection
  |
  v
CombinePerKey
  |
  v
Resultado
```

Esse fluxo representa **a lógica do processamento**.

### 4.2 Execução

Depois que o pipeline é definido, um Runner precisa executá-lo.

```text
DEFINIÇÃO

Pipeline
├── entrada
├── PCollections
├── PTransforms
└── saída

        |
        v

EXECUÇÃO

Runner
        |
        v

Infraestrutura escolhida
```

Portanto:

> O SDK é utilizado para descrever o pipeline; o Runner é utilizado para executá-lo.

Essa separação é uma das características mais importantes do Apache Beam.

---

## 5. O que é um Runner?

Um **Runner** é o componente responsável por executar um pipeline Apache Beam em uma plataforma específica.

Dependendo do Runner escolhido, o mesmo modelo de pipeline pode ser executado:

- no próprio computador;
- em um cluster Apache Flink;
- em uma infraestrutura Apache Spark;
- no serviço Google Cloud Dataflow.

Grande parte dos Runners funciona como uma camada de adaptação entre o modelo Beam e o mecanismo de processamento escolhido.

Exemplo:

```text
             Pipeline Beam
                   |
                   v
             Escolha do Runner
                   |
        +----------+----------+
        |          |          |
        v          v          v
      Flink      Spark     Dataflow
        |          |          |
        v          v          v
      Job        Job        Job
      Flink      Spark      Cloud
```

O Runner não define a regra de negócio. Ele executa a lógica que já foi descrita no pipeline.

Por exemplo:

```text
Filter: venda[1] > 0
```

A condição pertence ao `PTransform`.

O Runner é responsável por executar essa transformação dentro da infraestrutura escolhida.

---

## 6. Runner API e portabilidade

### 6.1 Problema de interoperabilidade

Sem uma camada de portabilidade, cada combinação entre linguagem e mecanismo de execução poderia exigir uma integração específica.

Por exemplo:

```text
Python  -> Flink
Python  -> Spark
Java    -> Flink
Java    -> Spark
Go      -> Flink
Go      -> Spark
```

Isso aumentaria significativamente o número de integrações necessárias.

### 6.2 Runner API

A **Runner API** representa o pipeline de forma independente do SDK utilizado para construí-lo.

Em nível introdutório, ela pode ser entendida como uma representação padronizada do pipeline que facilita a comunicação entre SDKs e Runners.

```text
Python SDK
Java SDK
Go SDK
    |
    v
Representação independente do SDK
Runner API
    |
    v
Runner
    |
    v
Mecanismo de execução
```

A documentação do Apache Beam define a Runner API como um esquema independente de SDK para representar um pipeline, acompanhado por interfaces relacionadas à execução e ao acompanhamento de jobs.

### 6.3 Portability Framework

A Runner API faz parte de um conjunto maior de mecanismos de portabilidade do Beam.

O **Portability Framework** estabelece estruturas e protocolos independentes de linguagem entre SDKs e Runners.

A ideia geral é:

```text
SDK
 |
 v
Representação portátil
 |
 v
Runner
 |
 v
Backend
```

Isso reduz o acoplamento entre a linguagem usada pelo desenvolvedor e a infraestrutura usada para executar o pipeline.

### 6.4 Observação sobre compatibilidade

Portabilidade não significa que todos os Runners implementam exatamente os mesmos recursos.

Cada Runner pode possuir:

- capacidades diferentes;
- versões suportadas diferentes;
- requisitos específicos;
- limitações próprias.

Por isso, o Apache Beam mantém uma **Capability Matrix** para comparação dos recursos implementados pelos diferentes Runners.

---

## 7. DirectRunner

### 7.1 Definição

O **DirectRunner** é voltado principalmente para execução local, desenvolvimento, testes e depuração.

Seu objetivo principal não é obter o melhor desempenho possível, mas ajudar a verificar se o pipeline está de acordo com o modelo do Apache Beam.

```text
Pipeline Beam
     |
     v
DirectRunner
     |
     v
Computador local
```

### 7.2 Principais usos

O DirectRunner é apropriado para:

- desenvolvimento inicial;
- aprendizado;
- depuração;
- testes unitários;
- pipelines pequenos;
- validação da lógica antes de executar em infraestrutura distribuída.

### 7.3 Limitações

O DirectRunner não é recomendado como mecanismo principal para grandes cargas de produção.

Entre as limitações estão:

- execução limitada pelos recursos da máquina local;
- memória disponível no computador;
- desempenho inferior ao de mecanismos distribuídos em grandes cargas;
- ausência da escalabilidade oferecida por clusters e serviços gerenciados.

### 7.4 Relação com o projeto

Durante a Fase 1 deste projeto, foi criado e validado um pipeline mínimo local.

O pipeline realizou:

```text
Create
  |
  v
Filter
  |
  v
CombinePerKey
  |
  v
Map(print)
```

A execução local comprovou que:

- o ambiente Apache Beam estava funcionando;
- as `PCollections` eram criadas corretamente;
- os `PTransforms` eram executados;
- o filtro descartava registros inválidos;
- a agregação por chave produzia o resultado esperado.

Nos testes realizados com Apache Beam 2.76.0, os logs locais apresentaram mensagens do `PrismRunner`. Esse detalhe representa uma implementação interna do caminho de execução local observado no ambiente e não altera a ideia arquitetural estudada nesta fase: o pipeline foi executado localmente por um Runner do ecossistema Beam.

Para a demonstração final, o grupo poderá tornar a escolha do Runner explícita no código quando isso for útil pedagogicamente.

---

## 8. Flink Runner

### 8.1 Definição

O **Flink Runner** permite executar pipelines Apache Beam utilizando o **Apache Flink** como mecanismo de execução.

```text
Pipeline Beam
     |
     v
Flink Runner
     |
     v
Apache Flink
```

### 8.2 Características

Segundo a documentação do Beam, o Flink é especialmente adequado para trabalhos contínuos e de grande escala.

Entre suas características estão:

- processamento batch;
- processamento streaming;
- alta taxa de processamento;
- baixa latência;
- tolerância a falhas;
- suporte a back-pressure;
- integração com ambientes distribuídos.

### 8.3 Modos de execução

O Flink Runner pode ser utilizado em diferentes ambientes, incluindo:

- modo local;
- clusters Flink;
- ambientes gerenciados por sistemas como Kubernetes ou YARN.

Para pipelines escritos em Python, a documentação do Beam utiliza a arquitetura portátil do Runner.

### 8.4 Quando faria sentido utilizar

Exemplos:

- processamento contínuo de eventos;
- telemetria;
- sensores;
- logs;
- sistemas de monitoramento;
- pipelines com grande volume de eventos e requisitos de baixa latência.

No nosso projeto, o Flink Runner será estudado conceitualmente. Não é necessário instalar um cluster Flink para cumprir o objetivo introdutório do seminário.

---

## 9. Spark Runner

### 9.1 Definição

O **Spark Runner** permite executar pipelines Apache Beam utilizando o **Apache Spark**.

```text
Pipeline Beam
     |
     v
Spark Runner
     |
     v
Apache Spark
```

### 9.2 Características

O Apache Spark é amplamente utilizado em processamento distribuído e análise de grandes volumes de dados.

O Spark Runner permite integrar o modelo Beam ao ecossistema Spark.

A documentação atual do Apache Beam apresenta diferentes implementações do Spark Runner, incluindo:

- implementação baseada em RDD/DStream;
- implementação relacionada ao Structured Streaming;
- Portable Runner.

### 9.3 Portabilidade

O Portable Spark Runner permite executar pipelines criados em diferentes SDKs, incluindo Python.

Entretanto, os recursos disponíveis dependem da implementação e da versão utilizada. Portanto, a compatibilidade deve ser verificada antes de escolher Spark para um ambiente real.

### 9.4 Quando faria sentido utilizar

Exemplos:

- organizações que já utilizam Apache Spark;
- processamento distribuído de grandes datasets;
- integração com um ecossistema de dados baseado em Spark;
- workloads batch;
- determinados cenários de streaming, conforme suporte do Runner utilizado.

---

## 10. Dataflow Runner

### 10.1 Definição

O **Dataflow Runner** permite executar pipelines Apache Beam utilizando o **Google Cloud Dataflow**, um serviço gerenciado do Google Cloud.

```text
Pipeline Beam
     |
     v
Dataflow Runner
     |
     v
Google Cloud Dataflow
     |
     v
Workers gerenciados na nuvem
```

### 10.2 Características

Ao utilizar o Dataflow Runner, o pipeline e suas dependências são preparados para execução no Google Cloud.

Entre os recursos oferecidos pelo serviço estão:

- infraestrutura gerenciada;
- execução distribuída;
- autoscaling;
- balanceamento dinâmico de trabalho;
- suporte a workloads contínuos;
- integração com outros serviços do Google Cloud.

### 10.3 Diferença em relação ao DirectRunner

```text
DirectRunner
→ execução local
→ desenvolvimento/teste
→ recursos do próprio computador

Dataflow Runner
→ execução na nuvem
→ serviço gerenciado
→ recursos distribuídos
→ escalabilidade
```

### 10.4 Quando faria sentido utilizar

Exemplos:

- pipelines de produção em Google Cloud;
- grandes volumes de dados;
- processamento contínuo;
- necessidade de escalabilidade automática;
- ambientes que desejam reduzir a administração direta da infraestrutura.

---

## 11. Comparação entre os Runners

| Runner | Ambiente | Execução distribuída | Batch | Streaming | Uso principal |
|---|---|---:|---:|---:|---|
| DirectRunner | Máquina local | Não como cluster de produção | Sim | Suporte para desenvolvimento/testes | Desenvolvimento, aprendizado e validação |
| Flink Runner | Apache Flink | Sim | Sim | Sim | Processamento distribuído e contínuo |
| Spark Runner | Apache Spark | Sim | Sim | Depende da implementação/capacidade | Ecossistema Spark e processamento distribuído |
| Dataflow Runner | Google Cloud Dataflow | Sim | Sim | Sim | Produção gerenciada em Google Cloud |

> Observação: as capacidades específicas podem variar conforme versão e implementação. Para uma decisão de produção, deve-se consultar a Capability Matrix oficial do Apache Beam.

---

## 12. Definição x execução: exemplo completo

Considere o seguinte processamento:

```text
Vendas
 |
 v
Remover valores inválidos
 |
 v
Agrupar por produto
 |
 v
Somar valores
 |
 v
Resultado
```

No Apache Beam:

### Definição

O desenvolvedor utiliza o SDK:

```python
pipeline
| beam.Create(vendas)
| beam.Filter(...)
| beam.CombinePerKey(sum)
```

Essa etapa descreve **o que deve acontecer**.

### Execução

O Runner é responsável por executar essa definição:

```text
                        Pipeline
                           |
             +-------------+-------------+
             |             |             |
             v             v             v
        DirectRunner   Flink Runner   Dataflow Runner
             |             |             |
             v             v             v
           Local          Flink         Cloud
```

Portanto, a lógica de negócio e o ambiente de execução são conceitos relacionados, mas não são a mesma coisa.

---

## 13. Arquitetura aplicada ao projeto do grupo

O projeto utiliza atualmente:

```text
Python 3.14.7
     |
     v
Apache Beam SDK for Python
     |
     v
Pipeline
     |
     +-- PCollection
     |
     +-- PTransforms
     |
     v
Execução local
     |
     v
Resultado
```

Essa escolha é adequada para a fase atual porque o objetivo é:

- compreender o modelo Beam;
- validar a lógica;
- construir demos reproduzíveis;
- reduzir dependências externas;
- evitar complexidade desnecessária durante o desenvolvimento.

Não é necessário utilizar Flink, Spark ou Dataflow para demonstrar o conceito de portabilidade.

O grupo pode apresentar os demais Runners como alternativas para ambientes de produção ou cenários de maior escala.

---

## 14. Por que o projeto começa com execução local?

A escolha de uma execução local na fase inicial foi intencional.

### Motivos

1. **Simplicidade**  
   Não exige cluster nem serviço de nuvem.

2. **Reprodutibilidade**  
   Todos os integrantes podem preparar o mesmo ambiente.

3. **Facilidade de depuração**  
   Erros podem ser identificados diretamente no computador.

4. **Baixo risco para a apresentação**  
   Reduz dependência de internet, credenciais e infraestrutura externa.

5. **Adequação ao nível introdutório**  
   O objetivo do seminário é demonstrar os conceitos fundamentais do Apache Beam.

6. **Evolução gradual**  
   Primeiro o grupo valida o modelo; depois desenvolve as demonstrações batch e streaming.

---

## 15. O que muda ao trocar de Runner?

A proposta do Beam é permitir maior portabilidade da lógica do pipeline.

Conceitualmente:

```text
Pipeline
  |
  +---- DirectRunner
  |
  +---- Flink Runner
  |
  +---- Spark Runner
  |
  +---- Dataflow Runner
```

Entretanto, trocar de Runner não significa que sempre será necessário alterar apenas uma palavra.

Na prática, podem existir diferenças em:

- configurações;
- dependências;
- credenciais;
- infraestrutura;
- opções de execução;
- recursos suportados;
- comportamento de determinadas funcionalidades;
- desempenho.

Por isso, a portabilidade do Beam reduz o acoplamento com o backend, mas não elimina a necessidade de validar o pipeline no Runner escolhido.

---

## 16. Resumo dos conceitos

### SDK

Ferramenta utilizada pelo desenvolvedor para escrever e definir o pipeline.

```text
SDK = construção da aplicação
```

### Pipeline

Representa a tarefa completa de processamento.

```text
Pipeline = fluxo definido
```

### Runner

Executa o pipeline em uma plataforma específica.

```text
Runner = mecanismo de execução
```

### Runner API

Representação independente de SDK utilizada no modelo de portabilidade do Beam.

```text
Runner API = representação portátil do pipeline
```

### DirectRunner

Execução local, indicada principalmente para desenvolvimento e testes.

### Flink Runner

Executa o pipeline utilizando Apache Flink.

### Spark Runner

Executa o pipeline utilizando Apache Spark.

### Dataflow Runner

Executa o pipeline utilizando o serviço gerenciado Google Cloud Dataflow.

---

## 17. Conclusão

A arquitetura do Apache Beam foi projetada para separar a lógica do processamento da infraestrutura que executa essa lógica.

Os **SDKs** permitem que o desenvolvedor defina pipelines utilizando abstrações comuns do Beam. O **Pipeline** representa a tarefa de processamento, enquanto as `PCollections` representam os dados e os `PTransforms` representam as operações.

Os **Runners** executam esse pipeline em diferentes ambientes. O DirectRunner é adequado para desenvolvimento e testes locais; o Flink Runner integra o Beam ao Apache Flink; o Spark Runner utiliza Apache Spark; e o Dataflow Runner executa o pipeline no serviço Google Cloud Dataflow.

A **Runner API** e o **Portability Framework** ajudam a reduzir o acoplamento entre SDKs e mecanismos de execução. Essa separação permite que a equipe concentre a lógica da aplicação no modelo Beam e escolha o backend de execução de acordo com os requisitos do ambiente.

No projeto deste seminário, a execução local foi escolhida por oferecer simplicidade, reprodutibilidade e baixo risco. Os demais Runners são apresentados como alternativas para cenários distribuídos e de produção.

---

## 18. Referências

APACHE BEAM. **Apache Beam Documentation**. Disponível em:  
https://beam.apache.org/documentation/

APACHE BEAM. **Beam Programming Guide**. Disponível em:  
https://beam.apache.org/documentation/programming-guide/

APACHE BEAM. **Basics of the Beam model**. Disponível em:  
https://beam.apache.org/documentation/basics/

APACHE BEAM. **Using the Direct Runner**. Disponível em:  
https://beam.apache.org/documentation/runners/direct/

APACHE BEAM. **Apache Flink Runner**. Disponível em:  
https://beam.apache.org/documentation/runners/flink/

APACHE BEAM. **Apache Spark Runner**. Disponível em:  
https://beam.apache.org/documentation/runners/spark/

APACHE BEAM. **Google Cloud Dataflow Runner**. Disponível em:  
https://beam.apache.org/documentation/runners/dataflow/

APACHE BEAM. **Runner Authoring Guide**. Disponível em:  
https://beam.apache.org/contribute/runner-guide/

APACHE BEAM. **Portability Framework Roadmap**. Disponível em:  
https://beam.apache.org/roadmap/portability/

Acesso em: 13 out. 2026.
