from pyspark.sql.functions import (
    col,
    trim,
    regexp_replace,
    when
)


def transform_reclamacoes(df):

    df = df.drop("Unnamed: 14")

    df = (
        df
        .withColumn(
            "instituicao_financeira",
            trim(col("Instituição financeira"))
        )
        .withColumn(
            "trimestre",
            trim(col("Trimestre"))
        )
    )

    df = df.withColumn(
        "cnpj_if",
        regexp_replace(
            trim(col("CNPJ IF")),
            "[^0-9]",
            ""
        )
    )

    df = df.withColumn(
        "total_reclamacoes",
        col(
            "Quantidade total de reclamações"
        ).cast("long")
    )

    df = df.withColumn(
        "clientes",
        col(
            "Quantidade total de clientes – CCS e SCR"
        ).cast("long")
    )

    return df