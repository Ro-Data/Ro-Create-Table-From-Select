<!-- -*- mode: gfm -*- -->

# create_table_from_select

This project provides an easy way to (re-)create a database table directly from
a SQL `SELECT` statement, without writing (or maintaining) any [DDL][]
(specifically, `CREATE TABLE` statements). Primary keys, foreign keys, and
[Amazon Redshift's][redshift] `DISTSTYLE`, `DISTKEY`, and `SORTKEY` features can
be used via optional YAML configuration files.

[DDL]: https://en.wikipedia.org/wiki/Data_definition_language
[redshift]: https://aws.amazon.com/redshift/

The `create_table_from_select.py` script can be used directly, or you can use
`create_table_from_select_operator.CreateTableFromSelectOperator` with Apache
Airflow.

The advantage is avoiding the need to explicitly write `CREATE TABLE` statements
for your derived tables, which then need to be updated whenever you add, remove,
or modify a column: update the source SQL, and the table will change as
necessary.

The only databases currently supported are PostgreSQL and Amazon Redshift.

## Requirements

- [`psycopg2`][psycopg2] is required
- [`pyyaml`][pyyaml] is required to use the (optional) ability to specify key
  information
- [`airflow`][airflow] is required to use the (optional)
  `CreateTableFromSelectOperator` or load PostgreSQL connection information from
  airflow

[psycopg2]: http://initd.org/psycopg/
[pyyaml]: https://pyyaml.org/
[airflow]: https://airflow.apache.org/

## Usage

### As a script

Given a SQL query in a file, such as:

```sql
-- /path/to/sql/my_orders_summary.sql
SELECT
    accounts.id,
    MIN(orders.created) AS first_order_created,
    MAX(orders.created) AS last_order_created,
    COUNT(orders.id) AS order_qty,
    SUM(orders.cost) AS total_cost
FROM
    accounts
    LEFT JOIN orders ON accounts.id = orders.account_id
GROUP BY
    accounts.id
```

To create a derived table based on this `SELECT` as
`myschema.my_orders_summary`, you could run:

```sh
python create_table_from_select.py              \
    --dbname mydb --host myhost.url --port 5432 \
    --user me --password myp4ssw0rd             \
    --sql-directory /path/to/sql                \
    myschema my_orders_summary
```

The script will drop and recreate the table (with appropriate DDL), no `ALTER
TABLE` or `DROP TABLE` / `CREATE TABLE` cycle required.

### As an Airflow operator

To do the same thing using an Airflow operator, you could define your DAG such
as:

```python
from airflow import DAG

from create_table_from_select_operator import CreateTableFromSelectOperator

default_args = {
    # ...
    'postgres_conn_id': 'my-postgress-conn-id',
    'sql_directory': '/path/to/sql'
}

dag = DAG(
    # ...
    default_args=default_args
)

my_orders_summary_op = CreateTableFromSelectOperator(
    schema_name='myschema',
    table_name='my_orders_summary',
    dag=dag
)
```

### Defining keys

Not having to update `CREATE TABLE` statements every time you add a column is
nice, but you might still want the ability to define primary and foreign keys.
If you're using Amazon Redshift, `DISTSTYLE`, `DISTKEY`, and `SORTKEY` are also
important.

To have these added to the DDL generated for your table, create a YAML file such
as:

``` yaml
# /path/to/sql/table_designs/my_orders_summary.yml
primary_key: id
diststyle: KEY
distkey: id
sortkey: [id]
```

This will cause the generated DDL to include:

``` sql
CREATE TABLE (
    -- ...
    PRIMARY KEY(id)
)
DISTSTYLE KEY
DISTKEY(id)
SORTKEY(id)
```

## Limitations

- The only databases currently supported are PostgreSQL and Amazon Redshift.
- In the course of creating the table, a temporary table is first created using
  `SELECT INTO`, so that column and type information can be extracted from
  `pg_table_def`. The script decides where to insert "`INTO [table-name]`" by
  searching for the last occurrence of "`FROM`". This means that:
  - If you don't follow the convention of capitalizing SQL keywords, the script
    won't be able to find the right place.
  - Even if you do capitalize SQL keywords, the script can be tricked if you
    happen to have an "`FROM`" after the appropriate one, perhaps in a comment.
- Building tables incrementally is not supported - the table is rebuilt from
  scratch each time.
