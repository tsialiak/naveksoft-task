docker load -i naveksoft_task.tar 
mkdir volume
tar -xzf clickhouse_backup.tar.gz -C volume
docker run -v %cd%/volume:/var/lib/clickhouse -d -p 8123:8123 --name clickhouse-server naveksoft-task:latest