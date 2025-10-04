# GYB — dbt mini-pipeline (PostgreSQL)

**Мета:** завантажити CSV із продажами, привести колонки до єдиного формату, розпарсити дати з різними локалями (включно з російськими назвами місяців), побудувати факт-таблицю та виконати аналітичні SQL-запити (MoM, KPI агентів, агенти з високим дисконтом).

**Стек:** Docker + PostgreSQL 16, dbt-postgres 1.10.x, SQL.

---

## 1) Швидкий старт

### 1.1 Підняти PostgreSQL у Docker
```bash
docker run --name pg-gyb -e POSTGRES_PASSWORD=pgpass -e POSTGRES_DB=gyb -p 5432:5432 -d postgres:16
docker ps   # контейнер має бути в Status: Up
```

### 1.2 Встановити dbt (локально)
> Тестувалось на: dbt-core/dbt-postgres 1.10.x, Python 3.13, Docker 28.x, PostgreSQL 16.
```bash
python -m pip install --upgrade pip
pip install dbt-postgres
dbt --version
```

### 1.3 Клонувати репозиторій та встановити пакети
```bash
git clone <цей-репозиторій>
cd gyb_test
dbt deps
```

### 1.4 Налаштувати профіль dbt (Postgres)
Створіть `~/.dbt/profiles.yml` (Windows: `C:\\Users\\<user>\\.dbt\\profiles.yml`) з таким вмістом:
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

### 1.5 Імпорт CSV і побудова моделей
```bash
dbt seed --full-refresh
dbt run
dbt test
```

Очікування: **Completed successfully** і успішні тести.

---

## 2) Структура проєкту

```
seeds/
└─ sales_data.csv          # вихідні дані (перейменовані колонки 1-в-1 як у ТЗ)

models/
├─ staging/
│  └─ stg_sales.sql        # очистка, нормалізація, парсинг дат (в т.ч. RU-місяці)
├─ intermediate/
│  └─ int_sales_by_reference.sql  # агрегації на рівні Reference ID
└─ marts/
   ├─ fct_sales.sql        # фінальна факт-таблиця
   └─ schema.yml           # тести якості (not_null, unique, accepted_values, діапазони)

analyses/
├─ revenue_mom.sql         # помісячний дохід + MoM %
├─ agent_kpis.sql          # KPI агентів (rank, середня знижка, тощо)
└─ high_discount_agents.sql# агенти зі знижкою > глобального середнього

dbt_project.yml
README.md
```

---

## 3) Що роблять моделі

**`models/staging/stg_sales.sql`**
- уніфікація текстових/порожніх значень: `coalesce(nullif(col,''), 'N/A')`;
- приведення сум до чисел: знімаємо `$`, «пробіли» тощо → `::numeric`;
- прапорці (refund/chargeback): `case when lower(val) in ('true','t','yes','y','1') then true …`;
- **парсинг дат** у різних форматах: `ISO`, `YYYY-MM-DD`, `YYYY-MM-DDThh:mm:ssZ`, `DD.MM.YYYY [hh:mm[:ss]]`, `MM/DD/YYYY hh:mm AM/PM` та **RU-місяці** (`январь 26, 2022, 8:11 PM`) → регуляркою замінюємо назву місяця на `MM`, далі `to_timestamp()`; повертає: `subscription_start_ts_kyiv`, `subscription_end_ts_kyiv`, `order_ts_kyiv`, `return_ts_kyiv`, `last_rebill_ts_kyiv` + числові поля, прапорці, текстові атрибути.

**`models/intermediate/int_sales_by_reference.sql`**
- узагальнює/рахує метрики на рівні `reference_id` (кількість замовлень, суми, середні знижки, тощо).

**`models/marts/fct_sales.sql`**
- формує **факт-таблицю** для аналітики (джерело для `analyses/*.sql`).  
- на цій таблиці виконуються тести із `schema.yml` (not_null/unique/accepted_values/діапазони).

---

## 4) Аналітичні запити (запуск)

Запити лежать у `analyses/` і можуть виконуватись напряму проти БД (у тебе вже є піднятий контейнер і schema `dbt_gyb`).

**Вивід у консоль (перевірити результати):**
```bash
# Revenue MoM
docker exec -i pg-gyb psql -U postgres -d gyb -f /tmp/revenue_mom.sql

# Agent KPIs
docker exec -i pg-gyb psql -U postgres -d gyb -f /tmp/agent_kpis.sql

# High discount agents
docker exec -i pg-gyb psql -U postgres -d gyb -f /tmp/high_discount_agents.sql
```

### (Опційно) Зберегти результати у CSV

**Варіант A — з хоста (PowerShell/Bash):**
```bash
# Revenue MoM
docker exec -i pg-gyb psql -U postgres -d gyb -c "\\copy ( $(cat /tmp/revenue_mom.sql) ) TO STDOUT WITH CSV HEADER"  > revenue_mom.csv

# Agent KPIs
docker exec -i pg-gyb psql -U postgres -d gyb -c "\\copy ( $(cat /tmp/agent_kpis.sql) ) TO STDOUT WITH CSV HEADER"  > agent_kpis.csv

# High discount agents
docker exec -i pg-gyb psql -U postgres -d gyb -c "\\copy ( $(cat /tmp/high_discount_agents.sql) ) TO STDOUT WITH CSV HEADER"  > high_discount_agents.csv
```

> Якщо PowerShell «ламає» лапки — скористайся варіантом B.

**Варіант B — всередині контейнера (гарантовано стабільно):**
```bash
docker exec -it pg-gyb bash

# У контейнері:
psql -U postgres -d gyb -c "\\copy ( $(cat /tmp/revenue_mom.sql) ) TO STDOUT WITH CSV HEADER" > /tmp/revenue_mom.csv
psql -U postgres -d gyb -c "\\copy ( $(cat /tmp/agent_kpis.sql) )  TO STDOUT WITH CSV HEADER" > /tmp/agent_kpis.csv
psql -U postgres -d gyb -c "\\copy ( $(cat /tmp/high_discount_agents.sql) ) TO STDOUT WITH CSV HEADER" > /tmp/high_discount_agents.csv
exit

# Скопіювати файли на хост:
docker cp pg-gyb:/tmp/revenue_mom.csv .
docker cp pg-gyb:/tmp/agent_kpis.csv .
docker cp pg-gyb:/tmp/high_discount_agents.csv .
```

> CSV-артефакти **не зберігаються** в репозиторії. За потреби їх легко відтворити командами вище.

---

## 5) Типові проблеми та рішення

- **UTF-8 BOM / кодування:** якщо файл зберігся як UTF-8 with BOM (Notepad), можуть «плавати» перші символи.  
  Відкрий у VS Code → `File → Save with Encoding → UTF-8`.
- **PowerShell/лапки:** якщо команди з `\\copy ( $(cat …) )` дають синтаксичні помилки — запусти «Варіант B» (всередині контейнера).
- **`dbt seed` помилка:** перевір заголовки `seeds/sales_data.csv` (мають збігатися з очікуваними в моделі), кодування файлу та роздільник — кома.

---

## 6) Результат для рев’ю (що зроблено)

- Повноцінний dbt-проєкт: **staging → intermediate → marts**.
- Стабільний **парсинг дат** (ISO/US/EU + RU-місяці) з нормалізацією значень, прапорцями та числовими полями.
- **Факт-таблиця** `fct_sales` як основа для аналітики.
- **Тести якості** (schema.yml): not_null, unique, accepted_values, діапазони.
- **Аналітичні SQL** (`analyses/*.sql`): MoM, KPIs агентів, агенти з високими знижками.
- **README** з відтворюваними кроками та опційним експортом у CSV.

---

## 7) Відтворюваність (однією командою на крок)

```bash
dbt deps && dbt seed --full-refresh && dbt run && dbt test
```

> Після цього аналітичні SQL (`analyses/*.sql`) можна виконувати одразу проти БД (див. розділ 4).

---

**Автор:** _Безугла Анастасія
