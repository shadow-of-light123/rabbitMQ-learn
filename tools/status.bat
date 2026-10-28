@echo off
REM 查看 RabbitMQ 运行状态
setlocal
set "TOOLS_DIR=%~dp0"
if "%TOOLS_DIR:~-1%"=="\" set "TOOLS_DIR=%TOOLS_DIR:~0,-1%"
set "ERLANG_HOME=%TOOLS_DIR%\erlang"
set "RABBITMQ_HOME=%TOOLS_DIR%\rabbitmq"
set "PATH=%ERLANG_HOME%\bin;%RABBITMQ_HOME%\sbin;%PATH%"
set "RABBITMQ_MNESIA_BASE=%TOOLS_DIR%\data\mnesia"
set "RABBITMQ_LOG_BASE=%TOOLS_DIR%\data\log"
set "RABBITMQ_NODENAME=rabbit@localhost"

echo === 端口监听情况 ===
netstat -ano | findstr "5672 15672"
echo.
echo === 节点状态 ===
call rabbitmqctl.bat status
echo.
echo === 队列消息数 ===
call rabbitmqctl.bat list_queues name messages 2>nul
endlocal
