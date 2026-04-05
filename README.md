![Graph View](screenshots/05_graph_view.png)

---

## Шаг 5: Загрузка и запуск DAG
```bash
# DAG автоматически подхватывается из папки dags/
airflow dags list
airflow dags trigger data_processing_pipeline
airflow dags list-runs -d data_processing_pipeline
```

![DAG в списке](screenshots/04_dag_list.png)

---

## Результаты выполнения

![Результаты](screenshots/06_results.png)

---

## Структура репозитория
'''
airflow-lab2/
├── README.md
├── dags/
│   └── my_first_dag.py
└── screenshots/
├── 01_airflow_version.png
├── 02_db_init.png
├── 03_airflow_ui.png
├── 04_dag_list.png
├── 05_graph_view.png
└── 06_results.png
'''
---

## Вывод

В ходе лабораторной работы был развёрнут Apache Airflow 2.10.3 на Ubuntu 24.04,
разработан DAG `data_processing_pipeline` с 4 задачами обработки данных,
DAG успешно поставлен на расписание и выполнен.