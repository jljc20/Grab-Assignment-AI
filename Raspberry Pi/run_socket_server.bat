cd /d %~dp0

cmd.exe /K "conda activate iot_env & python.exe socket_server.py 192.168.1.233 8889"

