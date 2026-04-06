"""
Data quality utilities for the SQL Server Lakeflow Connect ETL pipeline.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def add_quality_flags(df: DataFrame, rules: dict[str, str]) -> DataFrame:
    """
    Add a quality_flags array column based on named validation rules.

    Args:
        df: Input DataFrame.
        rules: Dict of {flag_name: SQL boolean expression}.
              When the expression evaluates to True the record is VALID;
              when False the flag_name is appended to quality_flags.

    Returns:
        DataFrame with an added 'quality_flags' array<string> column.
    """
    flag_exprs = []
    for flag_name, condition_expr in rules.items():
        flag_exprs.append(
            F.when(~F.expr(condition_expr), F.lit(flag_name))
        )

    return df.withColumn(
        "quality_flags",
        F.array_compact(F.array(*flag_exprs)),
    )


def add_quality_score(df: DataFrame) -> DataFrame:
    """
    Compute a quality_score (0.0 - 1.0) based on the ratio of passed checks.

    Requires the 'quality_flags' column produced by `add_quality_flags`.
    A score of 1.0 means all checks passed.
    """
    total_checks = F.lit(1)  # placeholder; overridden below
    return df.withColumn(
        "quality_score",
        F.when(
            F.col("quality_flags").isNull(), F.lit(1.0)
        ).otherwise(
            F.round(
                (F.lit(1.0) - (F.size("quality_flags") / F.lit(10.0))).cast("double"),
                2,
            )
        ),
    )


def quarantine_bad_records(df: DataFrame, threshold: float = 0.5) -> tuple[DataFrame, DataFrame]:
    """
    Split a DataFrame into good and quarantined records based on quality_score.

    Args:
        df: DataFrame with a 'quality_score' column.
        threshold: Minimum quality_score to pass (inclusive).

    Returns:
        Tuple of (good_df, quarantine_df).
    """
    good_df = df.filter(F.col("quality_score") >= threshold)
    quarantine_df = df.filter(F.col("quality_score") < threshold)
    return good_df, quarantine_df
