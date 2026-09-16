from pyspark.sql import DataFrame, SparkSession


TRIMESTRAIS = [
    "2021_tri_01.csv",
    "2021_tri_02.csv",
    "2021_tri_03.csv",
    "2021_tri_04.csv",
    "2022_tri_01.csv",
    "2022_tri_03.csv",
    "2022_tri_04.csv",
]


def extract_trimestrais(
    spark: SparkSession,
    base_path: str
) -> DataFrame:

    paths = [
        f"{base_path}/{arquivo}"
        for arquivo in TRIMESTRAIS
    ]

    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("inferSchema", True)
        .csv(paths)
    )

    return df