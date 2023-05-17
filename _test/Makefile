.PHONY: json
json: build
	cloudquery sync import-json.yaml --log-level=debug --telemetry-level=none

.PHONY: yaml
yaml: build
	cloudquery sync import-yaml.yaml --log-level=debug --telemetry-level=none

.PHONY: csv
csv: build
	cloudquery sync import-csv.yaml --log-level=debug --telemetry-level=none

.PHONY: xlsx
xlsx: build
	cloudquery sync import-xlsx.yaml --log-level=debug --telemetry-level=none


.PHONY: build
build:
	cd .. && make && cd _test

.PHONY: reset
reset:
	rm -rf *.log *.sqlite
