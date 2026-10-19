# Contexto e problema — Por que o Apache Beam existe?

## 1. Introdução

O Apache Beam é um framework de código aberto voltado à construção de pipelines de processamento de dados. Sua proposta central é oferecer um **modelo de programação unificado** para trabalhar com dados em lote (**batch**) e em fluxo contínuo (**streaming**), além de permitir que a mesma lógica de processamento seja executada por diferentes mecanismos por meio dos **Runners**.

O problema que o Beam procura resolver está relacionado à fragmentação das soluções de processamento de dados. Em muitos cenários, batch e streaming eram tratados com ferramentas, códigos e arquiteturas separadas, aumentando a complexidade do desenvolvimento e da manutenção.

## 2. Processamento batch e streaming

### Batch

O processamento batch trabalha com um conjunto finito de dados já armazenados.

```text
Vendas do mês
     ↓
Processamento
     ↓
Relatório financeiro
```

É comum em cargas periódicas, ETL, processamento de arquivos, relatórios históricos e atualização de data warehouses.

### Streaming

O processamento streaming trabalha com dados que chegam continuamente.

```text
Eventos de venda
      ↓
Processamento contínuo
      ↓
Atualização de indicadores
```

É comum em monitoramento, sensores, cliques, transações, telemetria e eventos de aplicações.

## 3. Limitações de manter soluções separadas

Quando uma organização utiliza abordagens completamente diferentes para batch e streaming, podem surgir:

- **duplicação de código**: a mesma regra de negócio precisa ser implementada mais de uma vez;
- **maior custo de manutenção**: alterações precisam ser aplicadas em sistemas diferentes;
- **dificuldade de integração**: ferramentas diferentes podem adotar modelos e estruturas distintas;
- **inconsistência de resultados**: implementações diferentes podem produzir resultados divergentes;
- **maior complexidade operacional**: a equipe precisa manter conhecimento e infraestrutura de várias tecnologias.

Exemplo: uma loja virtual pode precisar acompanhar vendas em tempo real e, ao mesmo tempo, gerar relatórios mensais. Sem um modelo unificado, regras semelhantes podem acabar sendo implementadas separadamente.

## 4. Necessidade de um modelo unificado

O Apache Beam propõe utilizar conceitos comuns para batch e streaming:

- `Pipeline`: representa o fluxo completo de processamento;
- `PCollection`: representa os dados processados;
- `PTransform`: representa as operações aplicadas aos dados;
- `Runner`: executa o pipeline em um ambiente específico.

```text
Fonte de dados
     ↓
PCollection
     ↓
PTransform
     ↓
Nova PCollection
     ↓
Saída
```

Essa abordagem facilita a reutilização da lógica de processamento e reduz a necessidade de criar aplicações completamente independentes para cada tipo de carga.

## 5. Portabilidade entre mecanismos de execução

Outro problema tratado pelo Apache Beam é a dependência de uma única plataforma de execução.

```text
Lógica do pipeline
        ↓
Runner
        ↓
Mecanismo de execução
```

Entre os Runners estudados no projeto estão:

- DirectRunner;
- Flink Runner;
- Spark Runner;
- Dataflow Runner.

Isso permite, por exemplo, desenvolver e testar um pipeline localmente e posteriormente executá-lo em outro ambiente compatível.

A portabilidade, porém, não significa que todos os Runners oferecem exatamente os mesmos recursos. Capacidades e limitações devem ser verificadas antes de uma migração.

## 6. Qual problema o Apache Beam pretende resolver?

O principal problema é a **fragmentação do processamento de dados**.

O Apache Beam busca:

- unificar batch e streaming em um modelo de programação comum;
- reduzir duplicação de lógica;
- padronizar a construção de pipelines;
- facilitar manutenção;
- separar lógica de processamento e infraestrutura de execução;
- aumentar a flexibilidade para escolha do Runner.

## 7. Exemplo prático

Imagine uma empresa de comércio eletrônico. Ela precisa acompanhar vendas conforme acontecem, gerar relatórios históricos, calcular indicadores e manter regras de negócio consistentes.

Com Apache Beam, a equipe pode utilizar conceitos comuns para processar tanto dados históricos quanto eventos contínuos.

## 8. Conclusão

O Apache Beam existe para tornar o processamento de dados mais padronizado, flexível e portátil. Sua principal proposta é permitir que batch e streaming sejam tratados dentro de um mesmo modelo, separando a lógica da aplicação da infraestrutura responsável pela execução.

## 9. Referências

- APACHE SOFTWARE FOUNDATION. **About Apache Beam**. https://beam.apache.org/about/
- APACHE SOFTWARE FOUNDATION. **Apache Beam Overview**. https://beam.apache.org/get-started/beam-overview/
- APACHE SOFTWARE FOUNDATION. **Basics of the Beam Model**. https://beam.apache.org/documentation/basics/
- APACHE SOFTWARE FOUNDATION. **Beam Programming Guide**. https://beam.apache.org/documentation/programming-guide/
- APACHE SOFTWARE FOUNDATION. **Portability Framework Roadmap**. https://beam.apache.org/roadmap/portability/

Acessos registrados nos materiais do grupo em outubro de 2026.
