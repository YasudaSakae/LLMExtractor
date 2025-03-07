###############################################################################
# execute_sql_script.py - Funções para conexão com PostgreSQL e execução de SQL
###############################################################################
import psycopg2
import os
import re

# Configurações do banco de dados (valores padrão, serão atualizados pela aplicação)
DB_CONFIG = {
    "host": "54.243.92.199",
    "port": 5433,
    "database": "compras_ia",
    "user": "compras",
    "password": "12345"
}

def execute_sql_script(sql_filepath):
    """
    Executa o script SQL conectando ao PostgreSQL.
    Retorna uma tupla (success, message) indicando sucesso e mensagem de resultado.
    """
    try:
        # Ler o arquivo SQL
        with open(sql_filepath, 'r', encoding='utf-8') as f:
            sql_script = f.read()
            
        # Conectar ao banco de dados PostgreSQL
        connection = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        
        # Informação para log
        results = []
        results.append(f"Conectado ao PostgreSQL em {DB_CONFIG['host']}:{DB_CONFIG['port']}, banco {DB_CONFIG['database']}")
        
        # Criar um cursor e executar o script
        with connection.cursor() as cursor:
            try:
                # Tentar executar o script completo
                cursor.execute(sql_script)
                affected_rows = cursor.rowcount
                results.append(f"Script executado com sucesso: {affected_rows} linhas afetadas")
            except Exception as script_error:
                # Se falhar ao executar tudo de uma vez, dividir por comandos
                connection.rollback()  # Desfaz qualquer alteração parcial
                
                # Este regex divide por ponto e vírgula, mas ignora os que estão dentro de strings
                sql_commands = re.split(r';(?=(?:[^\']*\'[^\']*\')*[^\']*$)', sql_script)
                
                # Contador de comandos bem-sucedidos
                success_count = 0
                error_count = 0
                
                for i, command in enumerate(sql_commands):
                    cmd = command.strip()
                    if cmd:  # Ignorar comandos vazios
                        try:
                            cursor.execute(cmd)
                            affected_rows = cursor.rowcount
                            results.append(f"Comando {i+1}: {affected_rows} linhas afetadas")
                            success_count += 1
                        except Exception as cmd_error:
                            # Registrar erro específico do comando
                            error_msg = str(cmd_error)
                            results.append(f"Erro no comando {i+1}: {error_msg}")
                            error_count += 1
                
                # Resumo da execução
                results.append(f"Resumo: {success_count} comandos bem-sucedidos, {error_count} erros")
                
                # Se todos os comandos falharam, considerar como falha geral
                if success_count == 0:
                    raise Exception(f"Todos os {error_count} comandos falharam")
            
            # Commit das alterações
            connection.commit()
            
        # Fechar conexão
        connection.close()
        
        result_message = "\n".join(results)
        return True, f"Script SQL executado com sucesso.\n{result_message}"
        
    except Exception as e:
        error_message = f"Erro ao executar script SQL: {str(e)}"
        print(error_message)
        
        # Se a conexão foi estabelecida, tentar fechar
        if 'connection' in locals() and connection:
            try:
                connection.rollback()  # Tentar rollback para garantir
                connection.close()
            except:
                pass
            
        return False, error_message

# Função para testar a conexão com o banco de dados
def test_database_connection():
    """
    Testa a conexão com o banco de dados usando as configurações definidas.
    Retorna uma tupla (success, message).
    """
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            connect_timeout=5
        )
        
        # Verificar versão do PostgreSQL para confirmar conexão
        with connection.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            
        # Se conseguiu conectar, conexão está ok
        connection.close()
        return True, f"Conexão com PostgreSQL estabelecida com sucesso.\nVersão: {version}"
        
    except Exception as e:
        error_message = f"Erro ao conectar ao PostgreSQL: {str(e)}"
        print(error_message)
        return False, error_message