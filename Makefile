export DBT_PROFILES_DIR := $(CURDIR)/dbt

.PHONY: install sample live ui test clean

install:
	pip install -r requirements.txt ruff

sample:        ## build everything from the committed three-state sample (no downloads)
	cd dbt && dbt parse
	USE_FIXTURES=1 dagster asset materialize -m hospital_quality.definitions --select '*'

live:          ## download the current CMS release and every past release, then build
	cd dbt && dbt parse
	dagster asset materialize -m hospital_quality.definitions --select '*'

ui:            ## Dagster UI with the asset graph at http://localhost:3000
	dagster dev -m hospital_quality.definitions

test:          ## dbt tests and unit tests only
	cd dbt && dbt build

clean:
	rm -rf data/warehouse.duckdb dbt/target dbt/logs
