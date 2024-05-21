from functools import singledispatchmethod

from datahub.api.entities.assertion.assertion_operator import (
    BetweenOperator,
    EqualToOperator,
    GreaterThanOperator,
    GreaterThanOrEqualToOperator,
    LessThanOperator,
    LessThanOrEqualToOperator,
    NotNullOperator,
    Operators,
)


class SnowflakeMetricEvalOperatorSQLGenerator:
    @singledispatchmethod
    def operator_sql(self, operators: Operators, metric_sql: str) -> str:
        raise ValueError(f"Unsupported metric operator type {type(operators)} ")

    @operator_sql.register
    def _(self, operators: EqualToOperator, metric_sql: str) -> str:
        return f"select case when metric=={operators.value} then 1 else 0 from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: BetweenOperator, metric_sql: str) -> str:
        return f"select case when metric between {operators.min} and {operators.max} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: LessThanOperator, metric_sql: str) -> str:
        return f"select case when metric < {operators.value} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: LessThanOrEqualToOperator, metric_sql: str) -> str:
        return f"select case when metric <= {operators.value} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: GreaterThanOperator, metric_sql: str) -> str:
        return f"select case when metric > {operators.value} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: GreaterThanOrEqualToOperator, metric_sql: str) -> str:
        return f"select case when metric >= {operators.value} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: NotNullOperator, metric_sql: str) -> str:
        return (
            f"select case when metric is not null then 1 else 0 end from ({metric_sql})"
        )
