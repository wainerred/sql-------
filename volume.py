import psycopg2
from psycopg2 import sql
from datetime import datetime
import paramiko

# Учетные данные для подключения к первой БД
pg_host_source = "msk-es04-app204"
pg_port_source = "5432"
pg_database_source = "journaling2_n"
pg_login_source = "repmgr"
pg_password_source = "wainer13"
ssh_port = 22
ssh_username = "nlab"
ssh_password = "CkbqUFGo"

# Учетные данные для подключения к второй БД
pg_host_dest = "msk-es04-app150"
pg_port_dest = "5432"
pg_database_dest = "KA"
pg_login_dest = "admin"
pg_password_dest = "Admin321"


# SQL запрос на получение информации о свободном месте в таблицах
sql_query = "SELECT  SUM(free_space) / (1024 * 1024 * 1024) AS GB FROM pg_statio_all_tables s, pgstattuple(s.schemaname || '.' || s.relname) AS st WHERE free_space > 0;"


# Установление соединения с первой БД
conn_source = psycopg2.connect(
    dbname=pg_database_source,
    user=pg_login_source,
    password=pg_password_source,
    host=pg_host_source,
    port=pg_port_source
)

cursor = conn_source.cursor()

# Выполнение SQL запроса на первой БД
cursor.execute(sql_query)
results = cursor.fetchall()

# Подключение к серверу msk-es04-app204 по ssh
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=pg_host_source, port=ssh_port, username=ssh_username, password=ssh_password)

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
    dbname=pg_database_dest,
    user=pg_login_dest,
    password=pg_password_dest,
    host=pg_host_dest,
    port=pg_port_dest
)
cursor_dest = conn_dest.cursor()

# Вставка результатов запроса в таблицу БД назначения
for row in results:
    cursor_dest.execute("INSERT INTO Volume (id, volume_bd) VALUES (DEFAULT, %s)", (row,))

# Запрос на свободное место на диске
#sql_disk_query = "SELECT pg_size_pretty(pg_tablespace_size('pg_default')) AS disk_space;"

# Выполнение запроса на первой БД
#cursor.execute(sql_disk_query)
#disk_space_result = cursor.fetchone()

# Получение времени завершения SQL скрипта
end_time = datetime.now()

# Удаляю симфол 'G'
total_space = int(total_space.rstrip('G'))  
used_space = int(used_space.rstrip('G'))
free_space = int(free_space.rstrip('G'))

# Вставка информации о дисковом пространстве в базу данных
cursor_dest.execute("INSERT INTO Volume (id, total_space, used_space, free_space) VALUES (DEFAULT, %s, %s, %s)", (total_space, used_space, free_space))



# Вставка данных о размере дискового пространства в таблицу второй БД
#cursor_dest.execute("INSERT INTO Volume (id, volume_disk) VALUES (DEFAULT, %s)", (disk_space_result,))

# Вставка времени завершения скрипта в таблицу второй БД
cursor_dest.execute("INSERT INTO Volume (id, date_end) VALUES (DEFAULT, %s)", (end_time,))

# Подтверждение изменений и закрытие соединений
conn_dest.commit()
conn_source.commit()
cursor.close()
cursor_dest.close()
conn_source.close()
conn_dest.close()