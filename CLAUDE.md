# CLAUDE.md — Movie Analytics Pipeline

## Sobre este projeto

Projeto de portfólio de Data Engineering usando o dataset público [GroupLens](https://grouplens.org/datasets/movielens/), recriando **100% localmente** uma arquitetura que o George já implementou em cloud (GCS + BigQuery + Metabase). O objetivo principal é **aprender as ferramentas fazendo**, não ter um projeto pronto.

**Referência conceitual:** [query-bigquery.sql](query-bigquery.sql) contém os modelos analíticos do projeto cloud original. Esse arquivo serve como **ponto de partida** — indica quais perguntas de negócio importam (top movies, genre performance, popularity vs quality, ratings heatmap, user activity). **Não é espec a replicar fielmente**: as queries originais são simples e podem ter imprecisões; a meta aqui é reescrever idiomaticamente em DuckDB + dbt com boas práticas, corrigindo o que fizer sentido e indo além se agregar valor.

---

## Perfil do usuário

- Python: pouca prática recente (lembra o básico)
- DuckDB: nunca usou
- dbt: usou pouco, lembra pouco
- Airflow: pouca experiência
- Docker: pouca experiência
- Ambiente: WSL2 (Ubuntu) com Python 3.12, projeto em `~/dev/netflix-data`
- Exploração de dados: DBeaver (Community Edition) conectado ao `data/warehouse.duckdb`

---

## Modo mentor — REGRAS OBRIGATÓRIAS

Estas regras se aplicam a TODA a sessão:

1. **Nunca escrever código de implementação completo.** Orientar, explicar, fazer perguntas — o usuário escreve o código.
2. Antes de gerar mais de ~5 linhas de lógica real, perguntar ao usuário se quer tentar sozinho primeiro.
3. Quando o usuário travar, dar uma **dica direcionada** — não a solução.
4. **Exceção à regra 1:** quando o usuário expressar **absoluta certeza** de que já sabe fazer algo, pode realizar a tarefa completa sem restrição.
4. Antes da implementação real, mostrar um **exemplo mínimo** (≤15 linhas, focado no conceito, não no projeto inteiro) + 2–3 observações sobre o essencial.
5. Explicar o "por quê" de cada ferramenta/decisão antes do "como".
6. Progredir uma fase de cada vez. Confirmar que o passo anterior funcionou antes de avançar.

---

## Mantras do projeto

Duas regras que eliminam 80% das decisões ambíguas:

1. **Staging é 1:1 com raw.** Só `CAST`, `TRIM`, rename para `snake_case`. Sem joins, sem agregação, sem explode, sem filtro. Se parece lógica de negócio, é `int_`.
2. **"Pronto" = `dbt run` + `dbt test` + `dbt docs generate` verdes + spot-check no DBeaver.** Spot-check obrigatório para marts; opcional para staging.

---

## Naming conventions

| Prefixo | Camada | Regra de nome |
|---|---|---|
| `stg_` | staging | Espelha o nome da tabela raw (ex: `stg_user_rating_history`) |
| `int_` | intermediate | Descreve a transformação (ex: `int_ratings_unified`) |
| `mart_` | marts | Descreve a pergunta de negócio (ex: `mart_top_movies`) |

Schemas no DuckDB: `raw`, `staging`, `intermediate`, `marts`. O schema `raw` precisa ser criado explicitamente no [ingestion/load_to_duckdb.py](ingestion/load_to_duckdb.py) — refactor da Fase 1.

---

## Stack do projeto

| Camada | Ferramenta | Substitui |
|---|---|---|
| Warehouse local | DuckDB | BigQuery |
| Transformação | dbt Core | SQL avulso no BigQuery |
| Orquestração | Apache Airflow | (não tinha antes) |
| BI | Apache Superset | Metabase |
| CI/CD | GitHub Actions | (não tinha antes) |

---

## Plano de aprendizado — Fases

### Fase 0: Setup e tradução conceitual
**Objetivo:** ambiente pronto e mapa mental BigQuery → DuckDB+dbt claro.

Entregáveis:
- `.venv` ativo, `dbt-duckdb` instalado, `dbt debug` passa
- DBeaver conectado em `data/warehouse.duckdb`
- Leitura das seções "Mapeamento BigQuery → dbt" e "Sintaxe BQ → DuckDB" abaixo

**Pronto quando:** `cd dbt_project && dbt debug` retorna "All checks passed" e você consegue rodar `SELECT * FROM movies LIMIT 5` no DBeaver.

---

### Fase 1: Ingestão (raw layer)
**Objetivo:** entender o que é um warehouse local e carregar CSVs em schema `raw` explícito.

Status atual: [ingestion/load_to_duckdb.py](ingestion/load_to_duckdb.py) ✅ existe, mas carrega no schema `main` e ingere 3 CSVs que o pipeline analítico não usa.

**Refactor pendente nesta fase:**
- Criar schema `raw` explicitamente (`CREATE SCHEMA IF NOT EXISTS raw`)
- Carregar **somente** os 3 CSVs que alimentam a camada analítica: `movies.csv`, `user_rating_history.csv`, `ratings_for_additional_users.csv`
- Parar de ingerir `belief_data.csv`, `movie_elicitation_set.csv`, `user_recommendation_history.csv` (nenhum mart consome — ver BQ→dbt abaixo)
- Path resolvido em relação ao arquivo do script (não ao `cwd`), senão o DAG Airflow quebra
- Logging básico (qual CSV, quantas linhas)

Conceitos-chave: conexão DuckDB, `read_csv_auto()`, schemas como namespace, idempotência (`CREATE OR REPLACE`), `pathlib` para path absoluto.

**Pronto quando:** DBeaver mostra 3 tabelas em `raw.*` e `COUNT(*)` de cada bate com `wc -l` do CSV menos 1.

---

### Fase 2a: Staging
**Objetivo:** camada de saneamento 1:1 com raw. Três modelos, nada mais.

Entregáveis:
- [dbt_project/models/staging/stg_movies.sql](dbt_project/models/staging/stg_movies.sql) ✅ existe — revisar à luz do mantra 1:1 (nota abaixo)
- `stg_user_rating_history.sql`
- `stg_ratings_for_additional_users.sql`
- [schema.yml](dbt_project/models/staging/schema.yml): descrições reais (placeholders `"a"`, `"b"`, `"c"` atuais são impostura), `sources` apontando para schema `raw`, testes `not_null` + `unique` nos PKs compostos

**Nota sobre `stg_movies`:** atualmente extrai `release_year` via regex, o que é lógica de limpeza defensável mas **não** é 1:1. Duas opções a decidir junto: (a) aceitar como "staging enriquecido" e documentar a exceção nos comentários; (b) mover a extração do year para `int_movies_clean`. Recomendação: (a), pois o regex é trivialmente determinístico.

**Pronto quando:** `dbt run --select staging` e `dbt test --select staging` verdes, `dbt docs generate` sem erro, DBeaver mostra 3 views em `staging.*`.

---

### Fase 2b: Intermediate e marts
**Objetivo:** reproduzir a camada analítica do BigQuery antigo.

Ordem obrigatória por dependência:

1. **`int_ratings_unified.sql`** — equivalente a `fact_ratings` do BQ. UNION ALL dos dois stg de ratings, parse de timestamp com fallback (`try_strptime` em dois formatos), filtro de NULLs. Coluna `src` para auditoria.
2. **`int_movie_kpis.sql`** — equivalente a `vw_movies_kpis`. LEFT JOIN entre `int_ratings_unified` e `stg_movies`, agregações por filme. (Mantém os 10.647 órfãos — decisão herdada do projeto antigo.)
3. **`mart_top_movies.sql`** — `total_rating >= 20`, `avg_rating BETWEEN 0 AND 5`, `ORDER BY avg_rating DESC, total_rating DESC`, `LIMIT 10`.
4. **`mart_popularity_vs_quality.sql`** — `total_rating >= 50` (scatter para Superset).
5. **`mart_ratings_heatmap.sql`** — `EXTRACT(YEAR/MONTH FROM rating_ts)` + count agrupado.
6. **`mart_genre_performance.sql`** — `CROSS JOIN unnest(string_split(genres, '|'))`, filtro `genres != '(no genres listed)'`, agregação por genre.
7. **`mart_user_activity.sql`** — agregação por `user_id`: count, distinct movies, avg, stddev, first/last activity.

**Testes adicionais** no `schema.yml` (ver seção "Testes a partir de insights do BQ"):
- `rating` entre 0 e 5
- `rating_ts` não-nulo e não-futuro
- `movie_id` em `int_ratings_unified` → `relationships` para `stg_movies` com **severity: warn**
- `release_year` entre 1900 e ano atual

**Pronto quando:** `dbt run` completo verde, `dbt test` verde (warns aceitáveis), 3 spot-checks no DBeaver comparando contagens/sumários com o BigQuery original.

---

### Fase 3: Docker + Airflow
**Objetivo:** orquestrar o pipeline end-to-end em container. **Vem antes de Superset** (decisão: Airflow é o item mais crítico para portfolio de DE; Superset é última milha).

Passos:
1. Docker crash-course focado: imagem, container, volume, rede, `compose up/down/logs/exec`.
2. Revisar [airflow/docker-compose-airflow.yml](airflow/docker-compose-airflow.yml): garantir mount do projeto em `/opt/app` **e** mount de `data/` (senão cada task recria warehouse zerado).
3. Ajustar [movie_pipeline_dag.py](airflow/dags/movie_pipeline_dag.py) se necessário (o script já usa `cd /opt/app` mas assume volume correto).
4. Subir Airflow, disparar DAG manualmente, debugar logs até verde.
5. Adicionar a 4ª task `dbt docs generate` (comentada como challenge no final do DAG).

**Pronto quando:** `movie_recommendation_pipeline` roda verde no UI + warehouse atualizado visível no DBeaver após o run.

---

### Fase 4: Superset
**Objetivo:** 3 dashboards consumindo marts (não 6 — qualidade > quantidade).

Passos:
1. `docker compose up` do Superset
2. Conectar em DuckDB via SQLAlchemy URI (`duckdb:////data/warehouse.duckdb`), compartilhando volume com o container Airflow
3. Criar datasets a partir de `marts.mart_*`
4. 3 dashboards priorizados:
   - **Movies:** Top Movies + Most Rated num único board
   - **Genres:** Genre Performance + Popularity vs Quality (scatter)
   - **Engagement:** Ratings Heatmap + User Activity distribution
5. Adicionar `exposures` no dbt apontando para cada dashboard — no lineage graph eles aparecem como consumidores finais.

**Pronto quando:** dashboards carregam; você quebra de propósito um mart (renomeia uma coluna) e confirma que o Superset falha — prova que o contrato staging→mart→dashboard está amarrado.

---

### Fase 5: CI/CD
**Objetivo:** portões automáticos de qualidade.

Passos:
1. Gerar `data/sample/` com 100 linhas de cada CSV (script Python curto)
2. [.github/workflows/ci.yml](.github/workflows/ci.yml): job que instala `dbt-duckdb`, roda `dbt run && dbt test` contra os samples
3. Adicionar `sqlfluff` (`dialect=duckdb`) — lint de todos os `.sql`
4. Opcional: pre-commit hook local com `sqlfluff`

**Pronto quando:** push num branch feature abre PR, CI roda, teste quebrado bloqueia merge.

---

## Mapeamento BigQuery → dbt

Baseado em [query-bigquery.sql](query-bigquery.sql) (projeto cloud original):

| BQ original | Tipo | Equivalente dbt | Camada |
|---|---|---|---|
| `netflix_raw.raw_movies` | external table | source `raw.movies` | raw |
| `netflix_raw.user_rating_history` | external table | source `raw.user_rating_history` | raw |
| `netflix_raw.ratings_for_additional_users` | external table | source `raw.ratings_for_additional_users` | raw |
| `netflix_raw.belief_data` | external table | **cortar** — nenhum mart consome | — |
| `netflix_raw.movie_elicitation_set` | external table | **cortar** | — |
| `netflix_raw.user_recommendation_history` | external table | **cortar** | — |
| `netflix_analytical.dim_movies` | table | `stg_movies` (colapsa em staging) | staging |
| `netflix_analytical.fact_ratings` | table | `int_ratings_unified` | intermediate |
| `netflix_analytical.vw_movies_kpis` | view | `int_movie_kpis` | intermediate |
| `netflix_analytical.vw_top_movies` | view | `mart_top_movies` | marts |
| `netflix_analytical.vw_scatter_popularity_vs_quality` | view | `mart_popularity_vs_quality` | marts |
| `netflix_analytical.vw_ratings_heatmap` | view | `mart_ratings_heatmap` | marts |
| `netflix_analytical.vw_genre_performance` | view | `mart_genre_performance` | marts |
| `netflix_analytical.vw_user_activity` | view | `mart_user_activity` | marts |

### Sintaxe BQ → DuckDB (cheat sheet)

| BigQuery | DuckDB | Onde aparece |
|---|---|---|
| `SAFE_CAST(x AS INT64)` | `TRY_CAST(x AS BIGINT)` | `fact_ratings`, `dim_movies` |
| `SAFE_CAST(x AS FLOAT64)` | `TRY_CAST(x AS DOUBLE)` | `fact_ratings.rating` |
| `SAFE.PARSE_TIMESTAMP(fmt, x)` | `try_strptime(x, fmt)` | `fact_ratings.rating_ts` |
| `COALESCE(SAFE.PARSE_TIMESTAMP(f1,x), SAFE.PARSE_TIMESTAMP(f2,x))` | `COALESCE(try_strptime(x, f1), try_strptime(x, f2))` | idem |
| `CROSS JOIN UNNEST(SPLIT(s, '\|'))` | `CROSS JOIN unnest(string_split(s, '\|')) AS t(genre)` | `vw_genre_performance` |
| `FORMAT_TIMESTAMP('%b', ts)` | `strftime(ts, '%b')` | `vw_ratings_heatmap` |
| `EXTRACT(YEAR FROM ts)` | idêntico | `vw_ratings_heatmap` |
| `REGEXP_EXTRACT(x, r'...', 1)` | `regexp_extract(x, '...', 1)` | `dim_movies.release_year` |
| `STDDEV(x)` | `stddev(x)` ou `stddev_samp(x)` | `vw_*` |
| `project.dataset.table` | `schema.table` (dataset ≡ schema) | todos |
| `QUALIFY` | idêntico (suportado) | — |

---

## Testes a partir de insights do BQ

O `query-bigquery.sql` deixou pistas explícitas do que vale testar:

- **"10.647 movie_ids em fact_ratings não existem em dim_movies"** (comentário no `vw_movies_kpis`). → `relationships` com `severity: warn` em `int_ratings_unified.movie_id` → `stg_movies.movie_id`. Warn, não error, porque a decisão original foi manter os órfãos.
- **Timestamp tem dois formatos** (`%Y-%m-%d %H:%M:%S%Ez` e `%Y-%m-%d %H:%M:%S`). → teste custom: `rating_ts` não-nulo em `int_ratings_unified` após o COALESCE.
- **Filtro `avg_rating BETWEEN 0 AND 5`** no `vw_top_movies` sugere ratings fora do range na origem. → `dbt_utils.expression_is_true` em `int_ratings_unified.rating` entre 0 e 5 (ou ampliar para 0 e 10 como sanity extra).
- **`rating_ts` no futuro** seria corrupção. → singular test: `select * from {{ ref('int_ratings_unified') }} where rating_ts > current_timestamp`.
- **`genres != '(no genres listed)'`** é exclusão explícita no `vw_genre_performance`. → aceitar em staging (é dado legítimo), filtrar em `mart_genre_performance`.
- **Filtro NULL no `fact_ratings`** (user_id, movie_id, rating, rating_ts, src). → `not_null` em cada coluna de `int_ratings_unified`.

---

## Estado atual do projeto

### Scaffold pronto (não refazer)
- `dbt_project/dbt_project.yml`
- `dbt_project/profiles.yml`
- `dbt_project/models/staging/schema.yml` — descrições placeholder; será reformado na Fase 2a
- `airflow/dags/movie_pipeline_dag.py` — revisar volumes na Fase 3
- `airflow/docker-compose-airflow.yml`, `docker/docker-compose-superset.yml`
- `Makefile`, `.env.example`, `.gitignore`, `.github/workflows/ci.yml`

### Lógica escrita
- ✅ `ingestion/load_to_duckdb.py` — **refactor pendente na Fase 1** (schema `raw`, cortar 3 CSVs, path absoluto, logging)
- ✅ `dbt_project/models/staging/stg_movies.sql` — revisar à luz do mantra 1:1 na Fase 2a (opção (a): aceitar como exceção documentada)

### Próximo passo concreto
**Fase 1 refactor** do `ingestion/load_to_duckdb.py` — pequeno, alta alavancagem, destrava toda a camada seguinte.

### Dados
- CSVs do GroupLens em `data/raw/` (os 3 não-usados ficam fisicamente ali, só não ingerimos mais)
- `data/warehouse.duckdb` atualmente com 6 tabelas em `main`; após o refactor serão 3 em `raw`

---

## Disciplina de git

- Um commit semanticamente nomeado ao final de cada subfase (ex: `feat(staging): stg_user_rating_history + tests`, `refactor(ingest): usar schema raw e cortar CSVs não usados`)
- Branch `feat/fase-X-*` → PR → merge faz sentido quando o CI estiver rodando (Fase 5). Antes disso, commits direto em `main` tudo bem.

---

## Como retomar a sessão

Diga ao Claude: *"Quero continuar o projeto de portfólio de Data Engineering. Leia o CLAUDE.md e me diga em qual fase estamos e qual é o próximo passo."*
