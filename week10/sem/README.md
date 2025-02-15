## Семинар 10 - BI-системы

#### 1) Postgres

В семинаре использовалась база интернет-магазина<br>
Она устроена так же, как и ваши ДЗ №1<br>
Тут должны быть команды для поднятия вашей БД<br>
```bash
docker-compose up -d postgres_primary
docker-compose up -d postgres_secondary
```

#### 2) Prometheus + Grafana

- `cd demo_prometeus_grafana_pg/`
- Смотрим в `docker-compose.yaml`
- Смотрим в конфиг Prometheus - `prometheus.yml`<br>

```yaml
global:
    scrape_interval: 15s
    evaluation_interval: 15s

scrape_configs:
    - job_name: prometheus
      static_configs:
        - targets: ['localhost:9090']

    - job_name: postgres-exporter
      static_configs:
        - targets: ['postgres-exporter:9187']
```
Здесь:
- scrape_interval - раз в какое время ходить к хостам и забирать метрики
- evaluation_interval - раз в какое время агрегировать метрики и дампить в хранилище
- scrape_configs - сервисы, куда мы ходим за метриками
- job_name - название сервиса
- targets - порт экспортера на стороне сервиса

- `docker-compose up -d`

- Что получим:
    1) Prometheus - localhost:9090
    2) Grafana - localhost:3000
    3) Добавляем подключение из Grafana в Prometheus
    4) https://grafana.com/grafana/dashboards/9628-postgresql-database/
    5) Добавляем подключение из Grafana в Postgres
    4) Играем с postgres

#### 3) Plotly Dash

- python3 -m venv plotly_env
- source plotly_env/bin/activate
- pip install -r requirements.txt

#### 4) Streamlit

- python3 -m venv streamlit_env
- source streamlit_env/bin/activate
- pip install streamlit
- streamlit hello


#### 5) Superset

- Инструкция - https://superset.apache.org/docs/installation/installing-superset-using-docker-compose/
- `git clone https://github.com/apache/superset.git`
- `git checkout 3.0.0`
- `docker compose -f docker-compose-non-dev.yml pull`
- `docker compose -f docker-compose-non-dev.yml up`

Как работать:
- Создаем подключение
- Создаем датасет, используя подключение
- Создаем чарт, используя датасет
- Создаем борд, используя чарты


#### 6) Metabase

- `docker pull metabase/metabase:latest`
- `docker run -d -p 12345:3000 --name metabase metabase/metabase`
- Остальное - справится даже ребенок

#### 7) Tableau

- https://public.tableau.com/app/profile/brian.moore7221/viz/RubiksCube/RubiksCube
- https://public.tableau.com/app/discover