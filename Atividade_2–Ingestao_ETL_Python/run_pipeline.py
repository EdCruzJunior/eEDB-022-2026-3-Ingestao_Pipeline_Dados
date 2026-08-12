from pathlib import Path
import subprocess
import sys


BASE_DIR = Path(__file__).resolve().parent

SCRIPTS = [
    BASE_DIR / "scr" / "01_ingestao_raw.py",
    BASE_DIR / "scr" / "02_trusted.py",
    BASE_DIR / "scr" / "03_delivery.py",
    BASE_DIR / "scr" / "04_carga_delivery.py",
]


def executar():

    for script in SCRIPTS:

        if not script.exists():
            print(
                f"ERRO: script nao encontrado: {script}"
            )
            sys.exit(1)

        print()
        print("=" * 70)
        print(f"Executando: {script.relative_to(BASE_DIR)}")
        print("=" * 70)

        resultado = subprocess.run(
            
            [
                sys.executable,
                str(script)
            ],
            cwd=BASE_DIR
        )

        if resultado.returncode != 0:

            print(
                "ERRO na execucao de "
                f"{script.relative_to(BASE_DIR)}"
            )

            sys.exit(
                resultado.returncode
            )

    print()
    print("=" * 70)
    print("PIPELINE EXECUTADO COM SUCESSO")
    print("=" * 70)


if __name__ == "__main__":

    executar()