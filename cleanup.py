"""
Database Queue Cleanup Automation
Intelligent cleanup system for stuck queue records with maintenance detection

Author: Diogo Alves Fragoso
Description: Monitors queue table and performs safe cleanup when deadlocks are detected,
             while avoiding interference with scheduled maintenance operations.
"""

#!/usr/bin/env python3
import pandas as pd
import psycopg2


def check_queue_status(host, dbname, user, password):
    """
    Check queue status with read-only connection
    
    Args:
        host (str): Database host
        dbname (str): Database name
        user (str): Database user (read-only)
        password (str): User password
    
    Returns:
        int: Number of stuck records in queue
    """
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password
    )
    cur = conn.cursor()
    
    # Query for stuck records (process 64, older than 5 minutes)
    query = """
    SELECT count(*)
    FROM servidor.srvfisfila_servidor a
    WHERE a.cod_processo = '64'
      AND a.hor_cadastro <= current_time - interval '5 minutes'
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    
    print("Resultado da consulta inicial:")
    print(df)
    
    qtd = df.iloc[0, 0]
    
    # Close connection completely
    cur.close()
    conn.close()
    
    return qtd


def perform_cleanup(host, dbname, user, password):
    """
    Perform queue cleanup with write connection
    
    Args:
        host (str): Database host
        dbname (str): Database name
        user (str): Database user (write access)
        password (str): User password
    """
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password
    )
    cur = conn.cursor()
    
    # SQL script with audit trail and safe deletion
    sql_fix = """
    -- Create audit table if not exists
    CREATE TABLE IF NOT EXISTS temp_schema.erro_srn_64(
        dta_excluido TIMESTAMP,
        cod_fila_servidor_excluido INTEGER
    );

    -- Log the record being deleted
    INSERT INTO temp_schema.erro_srn_64
        SELECT CURRENT_TIMESTAMP, MIN(p.cod_fila_servidor)
        FROM servidor.srvfisfila_servidor p
        WHERE p.cod_processo = '64';

    -- Delete with CTE for verification
    WITH linhas_deletadas AS (
        DELETE FROM servidor.srvfisfila_servidor f
        WHERE EXISTS (
            SELECT 1 FROM temp_schema.erro_srn_64 a
            WHERE f.cod_fila_servidor = a.cod_fila_servidor_excluido
        )
        RETURNING 1
    )
    SELECT CASE
        WHEN EXISTS (SELECT 1 FROM linhas_deletadas) THEN 'DELETE teve sucesso'
        ELSE 'DELETE não teve sucesso'
    END AS resultado;
    """
    
    cur.execute(sql_fix)
    conn.commit()
    
    print("Processo executado com sucesso.\n")
    
    cur.close()
    conn.close()


def main():
    """Main execution function"""
    
    # Database Configuration (read-only user)
    DB_HOST = '172.16.x.xxx'
    DB_NAME = 'Postgres'
    DB_USER_RO = 'readonly_user'
    DB_PASS_RO = 'readonly_password'
    
    # Database Configuration (write user)
    DB_USER_RW = 'readwrite_user'
    DB_PASS_RW = 'readwrite_password'
    
    # Check queue status
    qtd = check_queue_status(DB_HOST, DB_NAME, DB_USER_RO, DB_PASS_RO)
    
    # Intelligent decision logic
    if 0 < qtd < 5:
        # Stuck records detected - perform cleanup
        print(f"\nQuantidade ({qtd}) está entre 1 e 4. Rodando correção...\n")
        perform_cleanup(DB_HOST, DB_NAME, DB_USER_RW, DB_PASS_RW)
    else:
        # Either healthy (0) or maintenance (5+)
        print(f"\nQuantidade ({qtd}) não está entre 1 e 4. Nenhuma ação tomada.\n")


if __name__ == "__main__":
    main()
