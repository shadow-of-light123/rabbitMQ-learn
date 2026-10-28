@echo off
REM ===================================================================
REM  开启 RabbitMQ 管理插件（Web 管理台 :15672）
REM  注意：需要 RabbitMQ 正在运行（先在另一个窗口跑 start-rabbitmq.bat）
REM ===================================================================

setlocal
set "TOOLS_DIR=%~dp0"
if "%TOOLS_DIR:~-1%"=="\" set "TOOLS_DIR=%TOOLS_DIR:~0,-1%"

set "ERLANG_HOME=%TOOLS_DIR%\erlang"
set "RABBITMQ_HOME=%TOOLS_DIR%\rabbitmq"
set "PATH=%ERLANG_HOME%\bin;%RABBITMQ_HOME%\sbin;%PATH%"

set "RABBITMQ_MNESIA_BASE=%TOOLS_DIR%\data\mnesia"
set "RABBITMQ_LOG_BASE=%TOOLS_DIR%\data\log"
set "RABBITMQ_NODENAME=rabbit@localhost"

echo 正在开启 rabbitmq_management 插件...
call rabbitmq-plugins.bat enable rabbitmq_management

if errorlevel 1 (
    echo.
    echo [失败] 插件未开启。常见原因：RabbitMQ 服务没在运行。
    echo        请先在另一个窗口执行 start-rabbitmq.bat
    pause
    exit /b 1
)

echo.
echo ===================================================================
echo  管理台已开启: http://localhost:15672
echo  默认账号: guest / guest
echo ===================================================================
echo.
echo  提示: 插件已写入配置，之后每次启动都会自动生效，无需重复执行本脚本。
pause
endlocal
