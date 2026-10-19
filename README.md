<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a id="readme-top"></a>



<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform">
    <h3 align="center">AdventureWorks DataOps Platform</h3>
  </a>

  <p align="center">
    Apache Airflow 3.0 orchestration for the AdventureWorks sample dataset,
    loaded into Snowflake and served to reporting layers.
    <br />
    <a href="https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform"><strong>Explore the DAGs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform/tree/main/dags">Source code</a>
    &middot;
    <a href="https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform/issues/new?labels=bug">Report Bug</a>
    &middot;
    <a href="https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform/issues/new?labels=enhancement">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#data-architecture">Data Architecture</a></li>
        <li><a href="#pipeline-inventory">Pipeline Inventory</a></li>
        <li><a href="#repository-layout">Repository Layout</a></li>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li>
      <a href="#usage">Usage</a>
      <ul>
        <li><a href="#create-the-snowflake-connection">Create the Snowflake Connection</a></li>
        <li><a href="#running-the-cli-and-flower">Running the CLI and Flower</a></li>
        <li><a href="#adding-a-new-pipeline">Adding a New Pipeline</a></li>
      </ul>
    </li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li>
      <a href="#contributing">Contributing</a>
      <ul>
        <li><a href="#top-contributors">Top contributors:</a></li>
      </ul>
    </li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

This repository contains a working Airflow 3.0 deployment and a set of scheduled pipelines
built on top of the AdventureWorks sample dataset. The goal is to exercise a realistic
data-platform layout end to end: a DDL script that creates the AdventureWorks schema in
Snowflake, a data generator that fills it with realistic recent-dated rows, and a set of
analytical pipelines that read from the raw schema and write into a warehouse schema on a
cron-like cadence.

The orchestration stack is the stock Airflow 3 Compose deployment — `CeleryExecutor`,
Redis as the broker, PostgreSQL 17 for the metadata database, and the FAB auth manager —
with `dags/` bind-mounted into the containers. All heavy lifting is done by
`SQLExecuteQueryOperator` from `airflow.providers.common.sql` rather than the Snowflake
operator, which keeps the pipelines portable to any SQL engine that an Airflow connection
can reach. Python tasks are reserved for the steps that need branching logic, such as
pre-flight row-count checks and post-load quality validation.

Every pipeline follows the same shape: create-if-not-exists the target tables, validate
that source data exists for the period being processed, delete any rows already written
for that period so a re-run is idempotent, load the aggregation, validate the result, and
append a row to the execution log. None of the DAGs use `catchup`, so enabling one on a
fresh Airflow instance will not trigger a large historical backfill.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Data Architecture

```
AdventureWorks DDL (SQL_files/)
        |
        v
Snowflake dbt_db.dbt_schema          <- raw AdventureWorks tables
        |
        v
Snowflake dbt_db.dbt_warehouse       <- aggregates, quality results, execution logs
```

| Layer | Snowflake object | Role |
| --- | --- | --- |
| Raw | `dbt_db.dbt_schema.*` | AdventureWorks tables (`SALES_*`, `PERSON_*`, `PRODUCTION_*`, `PURCHASING_*`, `HUMANRESOURCES_*`) |
| Warehouse | `dbt_db.dbt_warehouse.*` | `daily_sales_summary`, `weekly_sales_forecast`, `data_quality_checks`, `data_anomalies`, `etl_log`, `etl_execution_log`, … |

The `dbt_db` / `dbt_schema` naming is a convention carried over from an earlier dbt-based
build of this project. dbt itself is not part of this repository — everything is plain SQL
executed through Airflow.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Pipeline Inventory

Seventeen DAGs ship in `dags/`. Every scheduled DAG declares `start_date=2024-08-01`,
`catchup=False`, `owner='data_team'`, and two retries with a five-minute delay.

| DAG | Schedule | Tags |
| --- | --- | --- |
| `daily_sales_etl` | `@daily` | sales, etl, daily |
| `data_quality_control` | `@daily` | data_quality, validation, daily |
| `currency_exchange_rates` | `@daily` | sales, currency, daily |
| `inventory_management` | `@daily` | inventory, stock, daily, operations |
| `sales_forecasting` | `@weekly` | sales, forecasting, weekly |
| `sales_person_performance` | `@weekly` | sales, performance, weekly |
| `sales_territory_analysis` | `@weekly` | sales, territory, weekly, analytics |
| `product_performance` | `@weekly` | product, performance, weekly, profitability |
| `production_costs` | `@weekly` | production, costs, weekly, optimization |
| `customer_analytics` | `@weekly` | customer, analytics, clv, segmentation, weekly |
| `customer_demographics` | `@weekly` | customer, demographics, geographic, segmentation, weekly |
| `customer_retention` | `@weekly` | customer, retention, cohort, churn, weekly |
| `marketing_campaigns` | `@weekly` | marketing, campaigns, weekly, targeting |
| `credit_card_analysis` | `@weekly` | credit_card, fraud, payment, weekly |
| `financial_reporting` | `@monthly` | financial, reporting, monthly, revenue |
| `populate_current_data` | manual only | setup, data, population |
| `populate_all_tables_2025_fixed` | manual only | setup, data, 2025 |

The two `populate_*` DAGs are the data generators. They are declared with
`schedule=None` and are meant to be triggered by hand once, to give the analytical DAGs
something recent to read.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Repository Layout

```
.
├── dags/                     # 17 Airflow DAGs, one file per pipeline
├── SQL_files/
│   ├── adventureworks_snowflake_final.sql   # full AdventureWorks DDL for Snowflake
│   ├── populate_adventureworks_data.sql     # seed inserts
│   └── populate_remaining_tables.sql        # seed inserts for the rest of the schema
├── config/airflow.cfg        # standalone config, mounted at /opt/airflow/config
├── Markdown_files/           # design notes: project analysis, ETL walkthrough,
│                             # Airflow concepts, cahier des charges
├── txt_files/                # scratch notes
├── docker-compose.yaml       # Airflow 3 + Postgres + Redis
├── Dockerfile                # single line: COPY dags/ /opt/airflow/dags/
├── .env                      # AIRFLOW_UID, AIRFLOW_IMAGE_NAME
├── requirements.txt          # empty; runtime deps are supplied via Compose
└── packages.txt              # empty; placeholder for OS-level packages
```

Two notes on the files that look misleading at first glance:

- `requirements.txt` and `packages.txt` are intentionally empty. Python dependencies are
  installed through `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yaml`
  (`pandas openpyxl xlrd psycopg2-binary`) rather than baked into the image. For anything
  beyond local experimentation, build a derived image instead.
- `config/airflow.cfg` is a standalone-development config: it sets `executor = LocalExecutor`,
  the SQLite metadata database, and the simple auth manager. `docker-compose.yaml` overrides
  all three through `AIRFLOW__*` environment variables, so the Compose stack runs
  `CeleryExecutor` on PostgreSQL with FAB auth even though the file says otherwise.

<p align="right">(<a href="#readme-top">back to top</a>)</p)



### Built With

* [![Apache Airflow][Apache Airflow]][Apache-Airflow-url]
* [![Python][Python]][Python-url]
* [![Snowflake][Snowflake]][Snowflake-url]
* [![PostgreSQL][PostgreSQL]][PostgreSQL-url]
* [![Redis][Redis]][Redis-url]
* [![Celery][Celery]][Celery-url]
* [![Docker][Docker]][Docker-url]
* [![Pandas][Pandas]][Pandas-url]

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

The deployment is the Airflow 3 Compose stack. Clone the repo, start the stack, wait for the
DAGs to be parsed, and register a Snowflake connection.

### Prerequisites

* Docker Engine with the Compose plugin (`docker compose version`)
  ```sh
  # macOS
  brew install --cask docker
  # Debian / Ubuntu
  sudo apt-get install -y docker.io docker-compose-v2
  ```
* A Snowflake account, because every task in `dags/` uses `conn_id='snowflake_conn'`.
* At least 4 GB of RAM and 2 free CPUs. `airflow-init` prints a warning and refuses to be
  quiet about it if the host is smaller.

### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform.git
   cd AdventureWorks-DataOps-Platform
   ```

2. Check the `.env` file. It pins the image tag and the UID that will own the mounted
   directories.
   ```sh
   AIRFLOW_UID=1000
   AIRFLOW_IMAGE_NAME=apache/airflow:3.0.2
   ```

3. Start the stack
   ```sh
   docker compose up -d
   ```
   The first run pulls `apache/airflow:3.0.2`, `postgres:17`, and `redis:7.2-bookworm`,
   then runs `airflow-init` to create the database schema and the admin user before any
   long-running service starts.

4. Open the Airflow UI at <http://localhost:8080> and sign in with `airflow` / `airflow`.
   Change `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` in `.env` before the
   first run if you do not want the defaults.

5. DAGs are created paused (`dags_are_paused_at_creation = True`). Unpause the ones you want
   to run, or trigger them manually from the DAG view.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->
## Usage

### Create the Snowflake Connection

`dags/` references a single connection, `snowflake_conn`. It is not defined in the repository,
so it has to be added once per environment in the Airflow UI under **Admin → Connections**:

* Connection type: Snowflake
* Connection ID: `snowflake_conn`
* Account, user, password, role, warehouse, and database filled in for your account

The SQL in the DAGs then does `USE ROLE ACCOUNTADMIN; USE DATABASE dbt_db; USE SCHEMA ...`,
so the role granted to the connection needs privileges on `dbt_db` and the ability to create
tables in `dbt_db.dbt_warehouse`.

A quick way to verify the wiring from the CLI:

```sh
docker compose exec airflow-apiserver airflow connections get snowflake_conn
docker compose exec airflow-apiserver airflow dags list
docker compose exec airflow-apiserver airflow dags list-import-errors
```

### Create the schema and load data

```sh
# 1. Apply the DDL in SQL_files/adventureworks_snowflake_final.sql with the Snowflake CLI
snowsql -c "$(cat SQL_files/adventureworks_snowflake_final.sql | tr '\n' ' ')"

# 2. Trigger the data generators (they are schedule=None, so manual trigger is the only way)
docker compose exec airflow-apiserver airflow dags trigger populate_all_tables_2025_fixed
docker compose exec airflow-apiserver airflow dags trigger populate_current_data
```

### Run a pipeline

```sh
docker compose exec airflow-apiserver airflow dags trigger daily_sales_etl
docker compose exec airflow-apiserver airflow dags list-runs -d daily_sales_etl
docker compose exec airflow-apiserver airflow tasks states-for-dag-run daily_sales_etl \
  manual__$(date +%Y-%m-%dT%H:%M:%S%z)
```

Because every pipeline deletes the target rows for the period it is about to write before
inserting them, a re-run of the same logical date is safe.

### Running the CLI and Flower

Two services are gated behind Compose profiles and do not start by default:

```sh
# One-off shell into a container running the Airflow CLI
docker compose --profile debug up airflow-cli

# Celery Flower task monitor on http://localhost:5555
docker compose --profile flower up flower
```

### Adding a New Pipeline

Add a file to `dags/` — the scheduler picks it up automatically through the bind mount. A
minimal pipeline that follows the conventions used throughout this repo:

```python
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id='my_pipeline',
    start_date=datetime(2024, 8, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'owner': 'data_team',
        'email_on_failure': False,
    },
    tags=['example'],
    description='What this pipeline does',
)
def my_pipeline():

    create_target = SQLExecuteQueryOperator(
        task_id='create_target',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;

        CREATE TABLE IF NOT EXISTS my_target (run_date DATE, value INTEGER);
        """,
    )

    delete_previous = SQLExecuteQueryOperator(
        task_id='delete_previous',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;

        DELETE FROM my_target WHERE run_date = CURRENT_DATE - 1;
        """,
    )

    load = SQLExecuteQueryOperator(
        task_id='load',
        conn_id='snowflake_conn',
        sql="""
        USE ROLE ACCOUNTADMIN;
        USE DATABASE dbt_db;
        USE SCHEMA dbt_warehouse;

        INSERT INTO my_target (run_date, value)
        SELECT CURRENT_DATE - 1, COUNT(*) FROM dbt_schema.SALES_SALESORDERHEADER;
        """,
    )

    create_target >> delete_previous >> load


my_pipeline()
```

Check the file parses before you rely on the scheduler picking it up:

```sh
docker compose exec airflow-apiserver airflow dags list-import-errors
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ROADMAP -->
## Roadmap

Derived from the state of the repository:

- [x] Airflow 3 Compose deployment (CeleryExecutor + Redis + PostgreSQL)
- [x] AdventureWorks DDL for Snowflake and seed scripts
- [x] Data-generation DAGs for current and fixed-date rows
- [x] Daily ETL, data-quality, currency-rate, and inventory pipelines
- [x] Weekly sales, product, production, customer, marketing, and credit-card analytics
- [x] Monthly financial reporting
- [ ] Notification layer (email or Slack on pipeline failure and on quality-check alerts).
      `Markdown_files/project_analysis.md` tracks this as phase 4; no notification operator
      or callback is wired up in `dags/` today.
- [ ] End-to-end tests. There is no `tests/` directory; quality assurance currently means the
      in-pipeline validation tasks only.
- [ ] A derived Docker image. The `Dockerfile` currently only copies `dags/`; the
      `_PIP_ADDITIONAL_REQUIREMENTS` pattern in `docker-compose.yaml` reinstalls dependencies
      on every container start and is explicitly documented as unsuitable beyond local use.
- [ ] Persist `config/airflow.cfg` secrets. `fernet_key`, `internal_api_secret_key`, and
      `jwt_secret` are committed defaults; generate real values for any shared environment.

See the [open issues](https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

Before opening a pull request against a new DAG, confirm it parses cleanly with
`airflow dags list-import-errors` against a local stack, and keep the conventions used by the
existing pipelines: `catchup=False`, `owner='data_team'`, `conn_id='snowflake_conn'`, and a
delete-before-insert step for the period being written.

### Top contributors:

<a href="https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Elouahabi-Naoufal/AdventureWorks-DataOps-Platform" alt="contrib.rocks image" />
</a>

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- LICENSE -->
## License

No license file is currently bundled with this repository. Until one is added, the default
GitHub terms apply and the repository should be treated as unlicensed — do not redistribute it
without asking. If you intend to build on these DAGs, open an issue or a pull request adding a
license.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Naoufal Elouahabi - [LinkedIn](https://www.linkedin.com/in/elouahabi-naoufal/) - elouahabi-naoufal1@gmail.com

Project Link: [https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform](https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [Apache Airflow](https://airflow.apache.org) — the `docker-compose.yaml` in this repository is
  derived from the official `docker-compose.yaml` shipped with Airflow 3.
* [Apache Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
* [Astronomer](https://www.astronomer.io) — `airflow-concepts-comparison.md` in
  `Markdown_files/` compares the Astronomer deployment model against self-hosting.
* [AdventureWorks sample database](https://learn.microsoft.com/en-us/sql/samples/adventureworks)
* [Snowflake SQLAlchemy dialect](https://docs.snowflake.com/en/user-guide/sqlalchemy.html)
* [Img Shields](https://shields.io)
* [Choose an Open Source License](https://choosealicense.com)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform.svg?style=for-the-badge
[contributors-url]: https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform.svg?style=for-the-badge
[forks-url]: https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform/network/members
[stars-shield]: https://img.shields.io/github/stars/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform.svg?style=for-the-badge
[stars-url]: https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform/stargazers
[issues-shield]: https://img.shields.io/github/issues/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform.svg?style=for-the-badge
[issues-url]: https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform/issues
[license-shield]: https://img.shields.io/github/license/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform.svg?style=for-the-badge
[license-url]: https://github.com/Elouahabi-Naoufal/AdventureWorks-DataOps-Platform
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/in/elouahabi-naoufal/
[Apache Airflow]: https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white
[Apache-Airflow-url]: https://airflow.apache.org/
[Python]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white
[Python-url]: https://www.python.org/
[Snowflake]: https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white
[Snowflake-url]: https://www.snowflake.com/
[PostgreSQL]: https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white
[PostgreSQL-url]: https://www.postgresql.org/
[Redis]: https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white
[Redis-url]: https://redis.io/
[Celery]: https://img.shields.io/badge/Celery-37814A?style=for-the-badge&logo=celery&logoColor=white
[Celery-url]: https://docs.celeryq.dev/
[Docker]: https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white
[Docker-url]: https://www.docker.com/
[Pandas]: https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white
[Pandas-url]: https://pandas.pydata.org/