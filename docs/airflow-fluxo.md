# Fluxo macro do pipeline — entendendo o que acontece

## Os 2 mal-entendidos principais

### Mal-entendido 1: "sobe um container quando precisa rodar"

**Errado.** Os containers do Airflow ficam **sempre rodando**, 24/7.

Pense neles como um servidor que você liga uma vez (`docker compose up`) e deixa ligado. O container do `airflow-scheduler` está sempre vivo, fazendo polling constante para ver se chegou hora de rodar algum DAG.

Containers só morrem se você:
- Mandar parar (`docker compose down`)
- Reiniciar a máquina
- Algum erro fatal

### Mal-entendido 2: "sobe o projeto para o container"

**Errado.** O projeto **nunca é copiado** para dentro do container.

O que existe é um **volume** — uma janela compartilhada entre sua máquina e o container. Quando o container olha para `/opt/app`, ele está vendo, em tempo real, a pasta `~/dev/netflix-data` da sua máquina. Se você editar um arquivo no VS Code, o container vê a mudança imediatamente.

```
SUA MÁQUINA                    CONTAINER
~/dev/netflix-data/   ←──→   /opt/app/
  ├── ingestion/                ├── ingestion/
  ├── dbt_project/              ├── dbt_project/
  └── data/                     └── data/
        ↑ MESMOS ARQUIVOS, sem cópia
```

---

## A arquitetura real — o que está sempre ligado

Quando você roda `docker compose -f airflow/docker-compose-airflow.yml up -d`, sobem 3 containers que ficam **permanentemente vivos**:

### 1. `postgres`
Banco de dados do próprio Airflow. Guarda:
- Quais DAGs existem e sua estrutura
- Histórico de execuções (todo run de toda task)
- Status atual (rodando, falhou, sucesso, esperando)
- Configurações do Airflow (conexões, variáveis)

**Importante:** este postgres não tem nada a ver com seus dados de filme. Ele é o "cérebro de memória" do Airflow.

### 2. `airflow-scheduler`
O coração do Airflow. Fica num loop infinito:

```
a cada poucos segundos:
  1. lê os arquivos .py em /opt/airflow/dags/
  2. verifica: algum DAG deveria estar rodando agora?
     - bateu o horário do schedule?
     - alguém clicou "Trigger" na UI?
  3. se sim, cria as task instances no postgres
  4. executa as tasks (no LocalExecutor, executa no próprio scheduler)
```

### 3. `airflow-webserver`
A UI web em `http://localhost:8080`. Não faz nada do pipeline — apenas mostra o estado do postgres em forma visual.

### Mais um container temporário: `airflow-init`
Roda **uma única vez** quando você faz `docker compose up` pela primeira vez. Cria as tabelas no postgres e o usuário admin. Depois morre. Não fica ligado.

---

## O fluxo passo a passo, do ponto de vista temporal

### Momento T0 — você liga tudo

```bash
docker compose -f airflow/docker-compose-airflow.yml up -d
```

O que acontece:
1. Docker baixa as imagens (só na primeira vez)
2. Sobe o `postgres` e espera ele ficar saudável
3. Roda o `airflow-init` (cria tabelas + usuário admin) e morre
4. Sobe o `airflow-webserver` e o `airflow-scheduler`
5. **Os 3 ficam ligados. Para sempre. Até você mandar parar.**

### Momento T1 — você abre o navegador

`http://localhost:8080` mostra a lista de DAGs disponíveis. O `movie_recommendation_pipeline` aparece porque o scheduler leu o arquivo `airflow/dags/movie_pipeline_dag.py` automaticamente.

### Momento T2 — algo dispara o DAG

Pode ser:
- **Manual:** você clica "Trigger DAG" na UI
- **Schedule:** o DAG tem `schedule="@daily"` e bateu meia-noite
- **Sensor:** algo detectou mudança numa fonte externa (não usamos aqui)

### Momento T3 — o DAG executa

O scheduler vê que precisa rodar. Cria 3 "task instances" no postgres:

```
1. ingest_csvs        (status: pending)
2. dbt_run            (status: pending)
3. dbt_test           (status: pending)
```

E começa a executar **na ordem definida pelas dependências**:

#### Task 1: `ingest_csvs`
- Comando: `cd /opt/app && python ingestion/load_to_duckdb.py`
- O scheduler executa esse comando dentro do próprio container do scheduler
- O Python lê os CSVs em `/opt/app/data/raw/` (que é a sua pasta `~/dev/netflix-data/data/raw/`)
- Grava em `/opt/app/data/warehouse.duckdb` (que é o seu `~/dev/netflix-data/data/warehouse.duckdb`)
- Status no postgres muda para `success` ou `failed`

#### Task 2: `dbt_run`
- Só roda se a task 1 terminou com `success`
- Comando: `cd /opt/app/dbt_project && dbt run --profiles-dir . --project-dir .`
- Roda os 9 modelos: 3 staging → 2 intermediate → 4 marts
- Cada modelo grava no `warehouse.duckdb`

#### Task 3: `dbt_test`
- Só roda se a task 2 terminou com `success`
- Comando: `cd /opt/app/dbt_project && dbt test --profiles-dir . --project-dir .`
- Roda todos os testes do `schema.yml`

### Momento T4 — DAG termina

Os containers **continuam ligados**. Não morre nada. O scheduler volta ao loop, esperando o próximo trigger.

---

## Resumindo: o que SEU mental model precisa ajustar

| Sua descrição original | Realidade |
|---|---|
| "Algo trigga a necessidade de subir um container" | Containers já estão sempre ligados |
| "Faz um ambiente ubuntu com dbt, duckdb" | A imagem `apache/airflow:2.9.1` já vem pronta; o `_PIP_ADDITIONAL_REQUIREMENTS` instala o `dbt-duckdb` no startup do container |
| "Sobe o projeto inteiro para o container" | O projeto está sempre acessível via volume; nada é copiado |
| "Airflow aciona algo para acabar com o container" | Containers nunca terminam sozinhos; só param quando você manda |

---

## Então o que muda quando o pipeline roda?

Apenas dados. Os arquivos do projeto (Python, SQL) não mudam — só são lidos e executados. O que muda é:

1. **`data/warehouse.duckdb`** — recebe novos dados
2. **Postgres do Airflow** — recebe registros de execução (essa task rodou, foi sucesso, demorou X segundos)
3. **Logs em `/opt/airflow/logs/`** — o que cada task imprimiu

Tudo o resto fica igual.

---

## Diagrama temporal

```
TEMPO →

[você] docker compose up
           │
           ▼
  ┌─────────────────────────────────────────────────────────┐
  │ postgres (sempre ligado)                                │
  │ airflow-scheduler (sempre ligado, em loop)              │
  │ airflow-webserver (sempre ligado)                       │
  └─────────────────────────────────────────────────────────┘
           │                    │                    │
           ▼                    ▼                    ▼
       [trigger 1]          [trigger 2]          [trigger 3]
           │                    │                    │
       executa DAG          executa DAG          executa DAG
       atualiza dados       atualiza dados       atualiza dados
           │                    │                    │
       containers           containers           containers
       continuam            continuam            continuam
       ligados              ligados              ligados
```

Cada execução do pipeline é só **um pulso de atividade** dentro de containers que estão sempre vivos.
