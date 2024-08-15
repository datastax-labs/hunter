## Schema

See [schema.sql](schema.sql) for the example schema.

## Usage

Define PostgreSQL connection details via environment variables:

```bash
export POSTGRES_HOSTNAME=...
export POSTGRES_USERNAME=...
export POSTGRES_PASSWORD=...
export POSTGRES_DATABASE=...
```

or in `hunter.yaml`.

The following command shows results for a single test `aggregate_mem` and updates the database with newly found change points:

```bash
$ BRANCH=trunk HUNTER_CONFIG=hunter.yaml hunter analyze aggregate_mem --update-postgres
```
