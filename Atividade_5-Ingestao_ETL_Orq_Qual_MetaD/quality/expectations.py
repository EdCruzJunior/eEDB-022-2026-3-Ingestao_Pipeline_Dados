# Catálogo das regras de qualidade utilizadas pela Atividade 5.
#
# A execução efetiva está em validate.py, usando Great Expectations.
#
# 1. A tabela Silver deve possuir pelo menos um registro.
# 2. ano não pode ser nulo e deve estar entre 2021 e 2022.
# 3. trimestre deve pertencer ao conjunto {1,2,3,4}.
# 4. instituicao_financeira não pode ser nula.
# 5. total_reclamacoes >= 0.
# 6. total_clientes_ccs_scr >= 0.
# 7. Regra de negócio: total_reclamacoes é a soma das três categorias
#    de reclamações.
