# Presentation Notes - Real-World Scenarios

Este arquivo nao faz parte da entrega tecnica do pipeline. Ele serve como apoio para a apresentacao do case e para discutir como a arquitetura poderia evoluir em um contexto de producao.

## 1. Mensagem principal

O case foi implementado com `Auto Loader` porque a fonte entregue no desafio e file-based: arquivos parquet organizados por tipo de taxi, ano e mes. Em um cenario real, eu nao escolheria a tecnologia de ingestao antes de entender:

- qual latencia o negocio precisa
- qual o volume diario e o pico esperado
- qual a frequencia de atualizacao da origem
- se o dado chega como evento, arquivo, CDC ou API
- se existe possibilidade de evento atrasado, duplicado ou corrigido

A arquitetura medalhao permanece valida. O que mais muda e a forma de ingestao e o contrato da Bronze.

## 2. Cenario atual do case: batch orientado a arquivos

Fonte:

- parquet em object storage

Escolha tecnica:

- `Auto Loader`
- micro-batch com `availableNow`
- Bronze append-only

Quando faz sentido:

- parceiro ou fornecedor entrega arquivos
- latencia de minutos/horas e suficiente
- reprocessamento historico e importante

Impacto por camada:

- Bronze: absorve schema drift, preserva dado de origem e metadados de ingestao
- Silver: aplica regras de qualidade, quarentena e padronizacao
- Gold: publica modelo canonicamente analitico e tabelas agregadas orientadas a consumo

## 3. Cenario real 1: eventos em Kafka

Fonte:

- topico Kafka com eventos de corrida

Exemplos de evento:

- corrida iniciada
- corrida finalizada
- tarifa corrigida
- cancelamento

Perguntas que eu faria:

- o evento representa snapshot ou mudanca de estado?
- existe `trip_id` unico na origem?
- posso receber eventos fora de ordem?
- qual a tolerancia para atraso?
- preciso de atualizacao em segundos, minutos ou horas?

Escolha tecnica esperada:

- `Structured Streaming`
- leitura de Kafka com `readStream`
- uso de `foreachBatch` para upsert nas camadas posteriores, quando necessario
- possivel integracao com schema registry

Impacto por camada:

- Bronze: muda bastante
- Silver: muda pouco conceitualmente, mas precisa tratar deduplicacao, late data e order of events
- Gold: pode continuar com a mesma ideia de modelagem, mudando apenas a frequencia de atualizacao

O que eu destacaria na apresentacao:

- em Kafka, a Bronze passa a ser uma camada de persistencia de eventos, com metadados como topic, partition, offset, event_time, ingestion_ts e payload bruto
- a Silver continua sendo a camada de contrato analitico e de qualidade
- a Gold continua sendo a camada de consumo

## 4. Cenario real 2: CDC de banco transacional

Fonte:

- captura de mudancas de um sistema operacional

Exemplos:

- insert de corrida
- update de valor total
- update de status
- delete/cancelamento

Escolha tecnica esperada:

- CDC para Kafka ou landing em Delta
- processamento incremental com semantica de mudanca

Impacto por camada:

- Bronze: armazena eventos de mudanca
- Silver: reconstrói estado corrente ou historico de mudancas, dependendo do objetivo
- Gold: segue muito parecida, desde que a Silver entregue o contrato correto

Mensagem importante:

- o problema deixa de ser apenas schema drift e passa a incluir semantica de negocio: insercao, alteracao, cancelamento e replay

## 5. Cenario real 3: arquitetura hibrida

Fonte:

- streaming para dado recente
- arquivos para backfill e historico

Exemplo:

- Kafka para eventos do dia
- object storage para fechamento diario e replay

Vantagens:

- baixa latencia para operacao
- caminho robusto para reconciliacao e reprocessamento

Cuidados:

- deduplicacao entre stream e backfill
- governanca de replay
- contratos claros entre ingestao incremental e full rebuild

## 6. O que eu manteria igual em quase todos os cenarios

- arquitetura medalhao
- Bronze como camada mais fiel a origem
- Silver como camada de qualidade e padronizacao
- Gold com fato no menor grao + agregados orientados a consumo
- separacao entre dado valido, dado em quarentena e regra analitica

## 7. O que eu mudaria conforme o SLA

### SLA analitico batch

Exemplo:

- dashboards atualizados 1x por hora ou 1x por dia

Escolha:

- processamento por arquivos ou micro-batch
- gold atualizada em janelas maiores

### SLA quase tempo real

Exemplo:

- monitoramento operacional da frota
- metricas atualizadas a cada poucos minutos

Escolha:

- streaming com micro-batches curtos
- tratamento explicito de watermark, late data e reprocessamento

### SLA tempo real estrito

Exemplo:

- alertas operacionais em segundos

Escolha:

- talvez Databricks continue no fluxo analitico, mas o operacional em tempo real pode exigir arquitetura complementar
- nesse caso eu avaliaria separar o plano operacional do plano analitico

## 8. Como conectar isso ao case

Mensagem pronta:

> "No desafio eu usei Auto Loader porque a origem disponibilizada era file-based e o objetivo principal era construir um pipeline medalhao confiavel sobre Databricks. Em um contexto real, eu escolheria a estrategia de ingestao a partir do contrato da fonte e do SLA de negocio. Se a origem fosse um topico Kafka, a principal mudanca estaria na Bronze e na semantica de ingestao. A Silver e a Gold continuariam muito parecidas conceitualmente, com ajustes para deduplicacao, late arriving events e frequencia de atualizacao."

## 9. Perguntas boas para levar para a entrevista

- Qual e o SLA esperado para disponibilizacao do dado?
- O caso de uso e operacional ou analitico?
- Existe identificador unico de corrida na origem?
- Posso receber eventos atrasados ou fora de ordem?
- Preciso guardar apenas o estado final ou tambem o historico de mudancas?
- Qual e a estrategia esperada de reprocessamento?
- O negocio aceita consistencia eventual?

## 10. Fechamento

O ponto principal a defender e:

- o case foi implementado de forma adequada ao contrato fornecido
- a arquitetura foi pensada para evoluir
- ingestao e a parte mais sensivel ao tipo de origem
- qualidade, contrato entre camadas e modelagem analitica seguem como fundamentos em qualquer cenario
