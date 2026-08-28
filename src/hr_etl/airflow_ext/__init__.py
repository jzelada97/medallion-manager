"""Airflow extensions for the HR ETL pipeline.

Contains custom deferrable sensors and their async triggers. These live inside the
`hr_etl` package (rather than the dags folder) so they are importable by dotted path
from every Airflow process — scheduler, dag-processor and, crucially, the triggerer,
which deserializes and runs the trigger's async loop.
"""
