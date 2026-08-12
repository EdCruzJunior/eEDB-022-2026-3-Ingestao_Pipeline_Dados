import psycopg2

print(psycopg2.__version__)

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="etl_python",
        user="etl_user",
        password="etl_password"
    )

    print("Conectado com sucesso!")
    conn.close()

except Exception as e:
    print(type(e))
    print(e)