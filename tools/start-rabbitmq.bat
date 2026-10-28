@echo off
REM ===================================================================
REM  启动 RabbitMQ（便携版，无需管理员权限，不写注册表）
REM
REM  原理：把本目录下的 erlang 和 rabbitmq 加入本次运行的 PATH，
REM        设置 ERLANG_HOME 和 RABBITMQ_HOME 指向它们，
REM        因此不会影响系统已安装的其他 Erlang/RabbitMQ。
REM
REM  关闭这个窗口 = 停止 RabbitMQ。
REM ===================================================================

setlocal

REM 脚本所在目录（tools\）
set "TOOLS_DIR=%~dp0"
REM 去掉末尾反斜杠
if "%TOOLS_DIR:~-1%"=="\" set "TOOLS_DIR=%TOOLS_DIR:~0,-1%"

set "ERLANG_HOME=%TOOLS_DIR%\erlang"
set "RABBITMQ_HOME=%TOOLS_DIR%\rabbitmq"

REM ---- 前置检查 ----
if not exist "%ERLANG_HOME%\bin\erl.exe" (
    echo [错误] 没找到 Erlang: %ERLANG_HOME%\bin\erl.exe
    echo         请先运行 setup.bat 完成解压。
    pause
    exit /b 1
)
if not exist "%RABBITMQ_HOME%\sbin\rabbitmq-server.bat" (
    echo [错误] 没找到 RabbitMQ: %RABBITMQ_HOME%\sbin\rabbitmq-server.bat
    echo         请先运行 setup.bat 完成解压。
    pause
    exit /b 1
)

set "PATH=%ERLANG_HOME%\bin;%RABBITMQ_HOME%\sbin;%PATH%"

echo ===================================================================
echo  RabbitMQ 便携版启动
echo ===================================================================
echo  ERLANG_HOME   = %ERLANG_HOME%
echo  RABBITMQ_HOME = %RABBITMQ_HOME%
echo.
echo  AMQP 端口     = 5672
echo  管理台        = http://localhost:15672  (guest / guest)
echo.
echo  数据目录      = %RABBITMQ_HOME%\..\data
echo  日志          = %RABBITMQ_HOME%\..\log
echo.
echo  按 Ctrl+C 或直接关闭窗口可停止服务。
echo ===================================================================
echo.

REM 把数据目录放在 tools 下（默认在 %APPDATA%，这里改到工程内便于清理）
set "RABBITMQ_MNESIA_BASE=%TOOLS_DIR%\data\mnesia"
set "RABBITMQ_LOG_BASE=%TOOLS_DIR%\data\log"

cd /d "%RABBITMQ_HOME%\sbin"

REM 启动。第一次运行会自动创建数据目录。
call rabbitmq-server.bat

endlocal
