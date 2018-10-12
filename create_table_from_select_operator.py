#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

import create_table_from_select


class CreateTableFromSelectOperator(PythonOperator):
    """(Re-)create a table based on a SQL query."""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        # Pop the `schema_name` and `table_name` from `kwargs`. We use them to
        # generate the `task_id` and then add them to `op_kwargs` so that
        # they're passed to the callable
        schema_name = kwargs.pop('schema_name')
        table_name = kwargs.pop('table_name')

        postgres_conn_id = kwargs.get('postgres_conn_id')
        if postgres_conn_id is None:
            postgres_conn_id = kwargs['dag'].default_args['postgres_conn_id']

        sql_directory = kwargs.pop('sql_directory', None)
        if sql_directory is None:
            sql_directory = kwargs['dag'].default_args['sql_directory']

        # Apply defaults for `task_id` and `provide_context`
        if 'task_id' not in kwargs:
            kwargs['task_id'] = 'create_{}_{}_task'.format(
                schema_name, table_name
            )
        if 'provide_context' not in kwargs:
            kwargs['provide_context'] = True

        # Set our hard-coded `python_callable`
        kwargs['python_callable'] = create_table_from_select.run_from_airflow
        # Stick `schema_name` and `table_name` into the `op_kwargs` that will
        # be passed to the callable
        kwargs['op_kwargs'] = kwargs.get('op_kwargs', {})
        kwargs['op_kwargs']['schema_name'] = schema_name
        kwargs['op_kwargs']['table_name'] = table_name
        kwargs['op_kwargs']['postgres_conn_id'] = postgres_conn_id
        kwargs['op_kwargs']['sql_directory'] = sql_directory

        super(CreateTableFromSelectOperator, self).__init__(*args, **kwargs)
