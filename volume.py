import psycopg2
from psycopg2 import sql
from datetime import datetime
import paramiko
import json

ssh_port = "22"
pg_port_source = "5432"
pg_port_dest = "5432"

with open('config.json', 'r') as config_file:
    config_data = json.load(config_file)

# SQL запрос на получение информации о свободном месте в таблицах
sql_query = "SELECT  SUM(free_space) / (1024 * 1024 * 1024) AS GB FROM pg_statio_all_tables s, pgstattuple(s.schemaname || '.' || s.relname) AS st WHERE free_space > 0;"


# Установление соединения с первой БД
conn_source = psycopg2.connect(
    dbname=config_data.get('pg_database_source'),
    user=config_data.get('pg_login_source'),
    password=config_data.get('pg_password_source'),
    host=config_data.get('pg_host_source'),
    port=pg_port_source
)

cursor = conn_source.cursor()

# Выполнение SQL запроса на первой БД
cursor.execute(sql_query)
results = cursor.fetchall()

# Подключение к серверу msk-es04-app204 по ssh
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=config_data.get('pg_host_source'), port=ssh_port, username=config_data.get('ssh_username'), password=config_data.get('ssh_password'))

# Команда для получения информации о диске
ssh_command = "df -h / | awk 'FNR == 2 {print $2,$3,$4}'"

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

# Удаляю символ 'G'
total_space = int(total_space.rstrip('G'))  
used_space = int(used_space.rstrip('G'))
free_space = int(free_space.rstrip('G'))


for row in results: # Вставка результатов запроса в таблицу БД назначения
    cursor_dest.execute("INSERT INTO Volume (id, volume_bd, total_space, used_space, free_space, date_end) VALUES (DEFAULT, %s, %s, %s, %s, %s)", (row, total_space, used_space, free_space, end_time)) 
    

# Подтверждение изменений и закрытие соединений
conn_dest.commit()
conn_source.commit()
cursor.close()
cursor_dest.close()
conn_source.close()
conn_dest.close()