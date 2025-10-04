# GYB — dbt mini-pipeline (PostgreSQL)

**Що це:** міні-проєкт з побудови аналітичного конвеєра на dbt + PostgreSQL.  
**Задача:** завантажити продажі з CSV, уніфікувати поля, **розпарсити дати з різних форматів і локалей** (включно з російськими назвами місяців), зібрати факт-таблицю та виконати три запити: **MoM виручки**, **KPI агентів**, **агенти з високими знижками**.

**Стек:** PostgreSQL 16 (Docker), dbt-postgres 1.10.x, SQL.

---

## Результат (коротко)

- Шари моделей: **staging → intermediate → marts**.  
- Стабільний **парсинг дат** (ISO, `DD.MM.YYYY`, `MM/DD/YYYY hh:mm AM/PM`, *ru*-місяці) з приведенням часу до `Europe/Kyiv`.  
- **Факт-таблиця `fct_sales`** як джерело для аналітики.  
- **Тести якості** (not_null, unique, accepted_values/діапазони) у `schema.yml`.  
- SQL-запити в `analyses/`:
  - `revenue_mom.sql` — помісячна виручка + % MoM;
  - `agent_kpis.sql` — дохід/ранг/середня знижка по агенту;
  - `high_discount_agents.sql` — агенти зі знижкою вище глобального середнього.

### Додані артефакти результатів (CSV)

- [`revenue_mom.csv`](./revenue_mom.csv) — помісячна виручка та **MoM %**.  
- [`agent_kpis.csv`](./agent_kpis.csv) — **KPI агентів**: кількість продажів, загальна виручка, середня знижка, ранк.  
- [`high_discount_agents.csv`](./high_discount_agents.csv) — **топ агентів за знижкою** (вище глобального середнього).

---

## Як влаштовано

### Структура
```
seeds/
  └─ sales_data.csv
models/
  staging/
    └─ stg_sales.sql                # очистка, нормалізація, парсинг дат (у т.ч. ru-місяці)
  intermediate/
    └─ int_sales_by_reference.sql   # агрегати на рівні reference_id
  marts/
    ├─ fct_sales.sql                # факт-таблиця
    └─ schema.yml                   # тести якості
analyses/
  ├─ revenue_mom.sql
  ├─ agent_kpis.sql
  └─ high_discount_agents.sql
revenue_mom.csv
agent_kpis.csv
high_discount_agents.csv
packages.yml
dbt_project.yml
README.md
```

### Логіка перетворень

**`stg_sales.sql` (staging)**  
- Пропуски → `coalesce(nullif(col,''),'N/A')`.  
- Суми: прибираю `$`, коми, пробіли → `::numeric`.  
- Прапорці (refund/chargeback): `case when lower(...) in (...)`.  
- **Парсинг дат:** гілки для ISO, `DD.MM.YYYY`, `MM/DD/YYYY hh:mm AM/PM`, і *ru*-місяців (попередня заміна назви місяця на номер + `to_timestamp`). Час — у `Europe/Kyiv`.

**`int_sales_by_reference.sql` (intermediate)**  
- Метрики на рівні `reference_id`: кількість, суми, середні знижки тощо.

**`fct_sales.sql` (marts)**  
- Єдине джерело для аналізу; на ньому запускаються тести зі `schema.yml`.

---

## Чому так (і що було найскладнішим)

- **Дати**: різні формати та локалі (включно з `январь/февраль…`). Для кожного формату — окрема гілка розпізнавання; далі уніфікація і TZ `Europe/Kyiv`.  
- **dbt** забезпечує структуру (staging→marts), повторюваність (`seed/run/test`) і тестування якості.

---

## Додаток A — Як відтворити локально (опційно)

> Цей блок для технічного відтворення; рекрутеру достатньо розділів вище.

1. **PostgreSQL (Docker)**
```
docker run --name pg-gyb -e POSTGRES_PASSWORD=pgpass -e POSTGRES_DB=gyb -p 5432:5432 -d postgres:16
```

2. **dbt (локально)**
```
python -m pip install --upgrade pip
pip install dbt-postgres
```

3. **Профіль dbt** — `~/.dbt/profiles.yml` (Windows: `C:\Users\<user>\.dbt\profiles.yml`)
```yaml
gyb_test:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: pgpass
      dbname: gyb
      schema: dbt_gyb
      port: 5432
      threads: 4
```

4. **Запустити пайплайн**
```
dbt deps
dbt seed --full-refresh
dbt run
dbt test
```

5. **Згенерувати CSV із `analyses/` (приклад для PowerShell)**
```
docker exec -it pg-gyb psql -U postgres -d gyb -c `
  "\copy ( $(Get-Content .\analyses\revenue_mom.sql -Raw) ) TO STDOUT WITH CSV HEADER" `
  > revenue_mom.csv
```

---

## Безугла Анастасія

© 2025, Анастасія Безуглая — SQL / dbt / аналітика даних.
