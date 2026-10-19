# Batch, Streaming, Windowing e Triggers no Apache Beam

## 1. Introdução

O Apache Beam oferece um modelo de programação unificado para processamento batch e streaming. Em ambos os casos, o desenvolvedor define um Pipeline, utiliza PCollections e aplica PTransforms. A principal diferença está na natureza dos dados e no comportamento temporal do processamento.

## 2. Batch

Batch trabalha com dados finitos.

Exemplos:

- arquivos CSV;
- cargas periódicas;
- ETL diário;
- dados históricos;
- atualização de data warehouse.

```text
Dados finitos
    ↓
Pipeline batch
    ↓
Processamento
    ↓
Resultado
    ↓
Fim
```

### Exemplo

```python
import apache_beam as beam

vendas = [
    {"loja": "Loja A", "valor": 100.0},
    {"loja": "Loja B", "valor": 50.0},
    {"loja": "Loja A", "valor": 80.0},
    {"loja": "Loja B", "valor": 70.0},
]

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Criar vendas" >> beam.Create(vendas)
        | "Transformar em chave e valor" >> beam.Map(
            lambda venda: (venda["loja"], venda["valor"])
        )
        | "Somar por loja" >> beam.CombinePerKey(sum)
        | "Exibir totais" >> beam.Map(print)
    )
```

Resultado esperado:

```text
('Loja A', 180.0)
('Loja B', 120.0)
```

## 3. Streaming

Streaming trabalha com dados que continuam chegando.

Exemplos: telemetria, eventos, transações, mensagens, cliques e sensores.

```text
Eventos
  ↓
PCollection unbounded
  ↓
Processamento contínuo
  ↓
Resultados parciais/atualizados
```

Uma PCollection streaming pode permanecer ativa sem um fim natural.

## 4. Bounded e unbounded

| Tipo | Descrição | Uso típico |
|---|---|---|
| Bounded | Conjunto finito | Batch |
| Unbounded | Fluxo contínuo | Streaming |

Batch e streaming utilizam o mesmo modelo de programação Beam.

## 5. Windowing

Em streaming, não é possível esperar que todos os dados terminem de chegar. Por isso, o Beam utiliza **janelas** para dividir o fluxo em grupos finitos.

```text
Fluxo contínuo
     ↓
Windowing
     ↓
Janela 1
Janela 2
Janela 3
     ↓
Agregações por intervalo
```

## 6. Fixed Windows

Janelas fixas possuem duração constante e não se sobrepõem.

```text
10:00 ───── 10:05
10:05 ───── 10:10
10:10 ───── 10:15
```

```python
from apache_beam.transforms.window import FixedWindows

pcoll = pcoll | beam.WindowInto(FixedWindows(300))
```

## 7. Sliding Windows

Janelas deslizantes podem se sobrepor.

Exemplo: duração de 5 minutos e avanço de 2 minutos.

```python
from apache_beam.transforms.window import SlidingWindows

pcoll = pcoll | beam.WindowInto(
    SlidingWindows(size=300, period=120)
)
```

Uso típico: calcular a cada dois minutos uma métrica dos últimos cinco minutos.

## 8. Session Windows

Janelas de sessão agrupam eventos próximos até ocorrer um período de inatividade.

```python
from apache_beam.transforms.window import Sessions

pcoll = pcoll | beam.WindowInto(Sessions(600))
```

São úteis para sessões de usuários, navegação e ciclos de interação.

## 9. Global Window

A Global Window reúne todos os elementos em uma única janela lógica. Em fluxos infinitos, uma agregação global precisa de uma estratégia adequada de trigger para produzir saídas úteis.

## 10. Triggers

Windowing responde: **em qual grupo temporal o evento pertence?**

Triggers respondem: **quando o resultado deve ser emitido?**

## 11. Tipos comuns de triggers

| Trigger | Condição |
|---|---|
| AfterWatermark | watermark passa do fim da janela |
| AfterProcessingTime | tempo de processamento avança |
| AfterCount | chega uma quantidade definida de elementos |
| AfterAny / AfterAll | combina condições |
| Early / Late | emissões antecipadas ou tardias |

## 12. Exemplo de trigger por quantidade

```python
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows

pcoll = pcoll | beam.WindowInto(
    FixedWindows(60),
    trigger=trigger.AfterCount(3),
    accumulation_mode=trigger.AccumulationMode.DISCARDING,
)
```

A ideia é emitir uma saída quando pelo menos três elementos forem observados.

## 13. Event time e processing time

### Event time

Momento em que o evento realmente aconteceu.

```text
Venda aconteceu às 10:02
```

### Processing time

Momento em que o sistema processou o evento.

```text
Venda foi processada às 10:08
```

Esses horários podem ser diferentes.

## 14. Watermark

Watermark é uma estimativa do progresso do tempo dos eventos.

```text
Watermark passou de 10:05
```

Não é uma garantia absoluta de que eventos antigos não aparecerão depois.

## 15. Late data

Late data é um evento que chega depois do momento esperado para sua janela.

```text
Evento ocorreu às 10:02
Janela terminou às 10:05
Evento chegou depois
```

## 16. Allowed lateness

Allowed lateness define por quanto tempo dados atrasados ainda podem ser aceitos.

```python
allowed_lateness=60
```

## 17. Acumulação

### DISCARDING

Após uma emissão, os elementos anteriores não compõem a próxima pane.

### ACCUMULATING

As emissões seguintes incluem os valores anteriores acumulados.

## 18. Comparação

| Necessidade | Abordagem |
|---|---|
| Carga de DW | Batch |
| Monitoramento de sensores | Streaming + Fixed Windows |
| Painel atualizado continuamente | Streaming + Windowing + Triggers |
| Atividade por usuário | Session Windows |
| Métrica móvel | Sliding Windows |

## 19. Relação com ETL e OLAP

```text
Extract
  ↓
Read
  ↓
Transform
  ↓
Map / Filter
  ↓
Aggregate
  ↓
CombinePerKey / WindowInto
  ↓
Load
```

Exemplos: leitura de CSV, filtragem de registros inválidos, normalização, agregação e gravação em destino analítico.

## 20. Recomendações para o projeto

Para a demonstração acadêmica:

- começar por uma demo batch simples;
- manter a fonte streaming controlada;
- evitar infraestrutura desnecessária;
- usar windowing de forma visual;
- mostrar claramente quando cada resultado é emitido;
- manter uma gravação de backup.

## 21. Conclusão

Batch processa conjuntos finitos. Streaming trabalha com fluxos contínuos. Windowing divide fluxos ilimitados em grupos temporais, enquanto triggers definem quando os resultados são emitidos.

Event time, processing time, watermark, late data e allowed lateness ajudam a explicar como o Beam lida com tempo e eventos atrasados.

## 22. Referências

- APACHE BEAM. **Programming Guide**. https://beam.apache.org/documentation/programming-guide/
- APACHE BEAM. **Python SDK**. https://beam.apache.org/documentation/sdks/python/
- APACHE BEAM. **Quickstart for Python**. https://beam.apache.org/get-started/quickstart/python/
- APACHE BEAM. **Basics of the Beam model**. https://beam.apache.org/documentation/basics/
- APACHE BEAM. **Python SDK Roadmap**. https://beam.apache.org/roadmap/python-sdk/

Acessos registrados nos materiais do grupo em outubro de 2026.
