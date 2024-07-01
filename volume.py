import psycopg2
from psycopg2 import sql
from datetime import datetime
import paramiko
import json

ssh_port = "22"
pg_port_source = "5432"
pg_port_dest = "5432"

with open('/home/vvay001/look/sql/config.json', 'r') as config_file:
    config_data = json.load(config_file)

# SQL запрос на получение информации о занятом пространстве таблицах
sql_query = "SELECT CAST((pg_database_size(current_database()) - (SELECT SUM(free_space) AS total_free_space FROM pg_statio_all_tables s, pgstattuple(s.schemaname || '.' || s.relname) AS st WHERE free_space > 0)) / (1024 * 1024 * 1024) AS double precision) AS total_size_in_gb_double;"


# Установление соединения с jornaling
conn_source_j2 = psycopg2.connect(
    dbname=config_data.get('pg_database_source_j2'),
    user=config_data.get('pg_login_source'),
    password=config_data.get('pg_password_source'),
    host=config_data.get('pg_host_source'),
    port=pg_port_source
)

cursor_j2 = conn_source_j2.cursor()

# Выполнение SQL запроса на jornaling
cursor_j2.execute(sql_query)
results = cursor_j2.fetchall()

# Установление соединения с analytics
conn_source_a = psycopg2.connect(
    dbname=config_data.get('pg_database_source_a'),
    user=config_data.get('pg_login_source'),
    password=config_data.get('pg_password_source'),
    host=config_data.get('pg_host_source'),
    port=pg_port_source
)
cursor_a = conn_source_a.cursor()

# Выполнение SQL запроса на analytics
cursor_a.execute(sql_query)
results_a = cursor_a.fetchall()

# Подключение к серверу msk-es04-app204 по ssh
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=config_data.get('pg_host_source'), port=ssh_port, username=config_data.get('ssh_username'), password=config_data.get('ssh_password'))

# Команда для получения информации о диске
ssh_command = "df -B1 / | awk 'FNR == 2 {print $2 / 1024 / 1024 / 1024 , $3 / 1024 / 1024 / 1024 , $4 / 1024 / 1024 / 1024 }'"

# Выполнение команды на сервере
stdin, stdout, stderr = ssh_client.exec_command(ssh_command)
disk_info = stdout.read().decode().strip().split()

# Разделение информации о диске на общий, занятый и свободный объем
total_space = disk_info[0]
used_space = disk_info[1]
free_space = disk_info[2]

# Закрытие SSH-соединения
ssh_client.close()

# Установление соединения с второй БД
conn_dest = psycopg2.connect(
    dbname=config_data.get('pg_database_dest'),
    user=config_data.get('pg_login_dest'),
    password=config_data.get('pg_password_dest'),
    host=config_data.get('pg_host_dest'),
    port=pg_port_dest
)
cursor_dest = conn_dest.cursor()

# Получение времени завершения SQL скрипта
end_time = datetime.now()



# Вставка полученных данных в таблицу базы данных
insert_query = sql.SQL("INSERT INTO storage (id, volume_db, total_space, used_space, free_space, date_end, volume_db_a2) VALUES (DEFAULT, {}, {}, {}, {}, {}, {});").format(
    #sql.Literal(results[0][0] if results else None),
    sql.Literal(results[1]),
    sql.Literal(total_space),
    sql.Literal(used_space),
    sql.Literal(free_space),
    sql.Literal(end_time),
    sql.Literal(results_a[1])
    #sql.Literal(results_a[0][0] if results_a else None)
)
# Выполнение SQL-запроса для вставки данных
cursor_dest.execute(insert_query)


#for row_j2, row_a in zip(results, results_a):
    #volume_db = row_j2[0]
    #volume_db_a2 = row_a[0]
    #cursor_dest.execute("INSERT INTO storage (id, volume_db, total_space, used_space, free_space, date_end, volume_db_a2) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s)", (row_j2, total_space, used_space, free_space, end_time, row_a)) 
    

# Подтверждение изменений и закрытие соединений
conn_dest.commit()
#conn_source.commit()
conn_source_a.commit()
conn_source_j2.commit()
#cursor.close()
cursor_dest.close()
cursor_j2.close()
cursor_a.close()
#conn_source.close()
conn_source_a.close()
conn_source_j2.close()
conn_dest.close()
