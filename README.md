# CloudQuery File Source Plugin

[![test](https://github.com/dihedron/cq-source-file/actions/workflows/test.yaml/badge.svg)](https://github.com/dihedron/cq-source-file/actions/workflows/test.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/dihedron/cq-source-file)](https://goreportcard.com/report/github.com/dihedron/cq-source-file)

A local file source plugin for CloudQuery that loads data from a file (in JSON, YAML, CSV or XLSX format) to any database, data warehouse or data lake supported by [CloudQuery](https://www.cloudquery.io/), such as PostgreSQL, BigQuery, Athena, and many more.

## Links

 - [CloudQuery Quickstart Guide](https://www.cloudquery.io/docs/quickstart)
 <!-- - [Supported Tables](docs/tables/README.md) -->


## Configuration

See [the CloudQuery Quickstart](https://www.cloudquery.io/docs/quickstart) for general information on how to configure the source and destination.

This source plugin can extract data from local files (given their path), apply some basic transformation and then provide it to CloudQuery for further processing and loading into the configured destinations.

You can find example configurations in the `_test/` folder, with specific setting for CSV, Microsoft eXcel, JSON and YAML.

The basic configuration is as follow:

```yaml
---
kind: source
spec:
  name: test1
  path: dihedron/file
  version: v0.1.0
  tables: 
    ["*"]
  destinations:
    - sqlite
  spec:
    file: ./test.csv
    format: csv
    separator: ","
    table: 
      name: T1
      filter: _.color startsWith 'b'
      columns:
        - name: color
          type: string
          key: true
          unique: true
          notnull: true
        - name: value
          type: string
          unique: true
          notnull: true
        - name: optimized
          type: boolean
          notnull: true
        - name: count
          type: integer
          notnull: true
    relations:
      - name: T1_UPPER
        filter: _.color startsWith 'blu'
        columns:
          - name: upper_color
            type: string
            key: true
            unique: true
            notnull: true
            transform: "{{index .Row \"color\" | toString | upper}}"
          - name: upper_value
            type: string
            transform: "{{index .Row \"value\" | toString | upper}}"
      - name: T1_UPPER
        filter: _.color startsWith 'bla'
        columns:
          - name: upper_color
            type: string
            key: true
            unique: true
            notnull: true
            transform: "{{index .Row \"color\" | toString | upper}}"
          - name: upper_value
            type: string
            transform: "{{index .Row \"value\" | toString | upper}}"
```

This source plugin does not export tables per se. You have to provide the information about the tables and colums that it should extract from the input file.

Thus, the plugin can export any kind of table, along with dependent tables ("relations") based on the metadata that has been provided in the plugin specific `spec` section.

The first part of the configuration file sepcifies that the plugin (`dihedron/file`) at version `v0.1.0` (the latest) should import all tables into an `sqlite` destination in CloudQuery.

The following `spec` provides plugin-specific configuration:

1. first of all it specifies the path to the file that provides the information to be imported into CloudQuery (`./test.csv`), that the file format is `csv` (other supported values are `json`, `yaml` and `xlsx`) and that the records are separated by a `,` (only required for `csv`); CSV and XLSX files *must* provide a header row that is used to name the columns, whereas JSON and YAML files must be arrays of objects (i.e. start with `[]` or with a list element `- ...`); the column (CSV, XLSX) or object attribute (YAML; JSON) names are used in the following column specification section;
2. secondly, the configuration specifies the main table to be imported; each table has a `name` and can provide an optional `filter`, which is an expression that is applied to each row and should return either `true` (in which case the row is sent to CloudQuery) or `false` (the row is dropped); the expression is based on [this rule engine grammar](https://github.com/expr-lang/expr); the current row is addressed via the `_` identifier and the fields are accessed as properties (`_.color`);
3. the table's colums are enumerated and decribed; each column has a `name`, a `type`, and can additionally specify whether it is part of the primary key (`key: true`), whether it has unique values (`unique: true`) and is non nullable (`notnull: true`); moreover, there is a `transform` property that provides a way to transform (or set to a constant value) the extracted value, both for data cleansing or for conditional extraction, according to Golang templates syntax; The templating engine has the whole of [Sprig](http://masterminds.github.io/sprig/) functions available;
4. last, a table can have dependent tables (`relations`), which are weak entities that are related to the main one; relations are useful when a single line in a CSV, XLSX, YAML or JSON file actually embeds multiple entities, e.g. a host (`hostname`, `serial`, `ram`, `cpus` ...) and its (possibly multiple) dependent NICs (`mac_address`, `ip_address`, `port_type` ...).
 
You can declare the main table (e.g. table `hosts`) and the dependent entities (e.g. a table for the host NICs, `host_nics`) separately and then instruct the plugin to extract the different entities -- host and nics, even in 1:N cardinality -- automatically.

Refer to the provided tests to see how this mechanism works.

## Development backlog

The source plugin is pretty feature complete.

It may be useful to add support for URLs in the `file` field (which would probably be deprecated in favour of a more generic `source` property) in order to have the plugin retrieve the file at the given URL prior to importing; support should be extended to at least HTTP(s), FTP, S3 and git URLs in addition to local files `file://`.

The implementation could leverage [Hashicorp's go-getter library](https://github.com/hashicorp/go-getter).

### Run tests

```bash
make test
```

### Run linter

```bash
make lint
```

### Generate docs

```bash
make gen-docs
```

### Release a new version

1. Run `git tag v1.0.0` to create a new tag for the release (replace `v1.0.0` with the new version number)
2. Run `git push origin v1.0.0` to push the tag to GitHub  

Once the tag is pushed, a new GitHub Actions workflow will be triggered to build the release binaries and create the new release on GitHub.
To customize the release notes, see the Go releaser [changelog configuration docs](https://goreleaser.com/customization/changelog/#changelog).
