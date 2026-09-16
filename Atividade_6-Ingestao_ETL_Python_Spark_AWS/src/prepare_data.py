from pathlib import Path


RAW = Path("/opt/project/data/raw")
OUTPUT = Path("/opt/project/data/utf8")

OUTPUT.mkdir(parents=True, exist_ok=True)


arquivos = list(RAW.iterdir())


for arquivo in arquivos:

    if arquivo.suffix.lower() not in [".csv", ".tsv"]:
        continue

    destino = OUTPUT / arquivo.name

    with open(
        arquivo,
        "r",
        encoding="cp1252"
    ) as origem:

        conteudo = origem.read()

    with open(
        destino,
        "w",
        encoding="utf-8",
        newline=""
    ) as destino_file:

        destino_file.write(conteudo)

    print(
        f"Convertido: {arquivo.name}"
    )