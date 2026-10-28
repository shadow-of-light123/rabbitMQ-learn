@echo off
REM ===================================================================
REM  一键准备：解压 Erlang 和 RabbitMQ 到本目录
REM  跑一次即可，之后用 start-rabbitmq.bat 启动。
REM ===================================================================

setlocal
set "TOOLS_DIR=%~dp0"
if "%TOOLS_DIR:~-1%"=="\" set "TOOLS_DIR=%TOOLS_DIR:~0,-1%"
cd /d "%TOOLS_DIR%"

echo ===================================================================
echo  RabbitMQ 便携版 环境准备
echo ===================================================================
echo.

REM ---- 检查 zip 是否都在 ----
if not exist "erlang.zip" (
    echo [错误] 缺少 erlang.zip
    echo         请确认已下载 OTP 28.2 的 otp_win64_28.2.zip 并重命名为 erlang.zip
    echo         （RabbitMQ 4.3.6 支持 Erlang 27~28，不能用 29）
    pause
    exit /b 1
)
if not exist "rabbitmq.zip" (
    echo [错误] 缺少 rabbitmq.zip
    pause
    exit /b 1
)

REM ---- 找一个可用的解压工具 ----
set "UNZIP="
where tar >nul 2>&1 && set "UNZIP=tar"
if not defined UNZIP (
    where powershell >nul 2>&1 && set "UNZIP=powershell"
)
if not defined UNZIP (
    echo [错误] 找不到 tar 或 powershell，无法解压。
    pause
    exit /b 1
)

REM ---- 解压 Erlang ----
if exist "erlang\bin\erl.exe" (
    echo [跳过] Erlang 已解压
) else (
    echo [1/2] 正在解压 Erlang ...  ^(约 200MB，需要一两分钟^)
    if exist "erlang" rmdir /s /q "erlang"
    mkdir "erlang"
    if "%UNZIP%"=="tar" (
        tar -xf "erlang.zip" -C "erlang"
    ) else (
        powershell -NoProfile -Command "Expand-Archive -Path 'erlang.zip' -DestinationPath 'erlang' -Force"
    )
    if not exist "erlang\bin\erl.exe" (
        echo [错误] Erlang 解压后结构不对，找不到 erlang\bin\erl.exe
        echo         请检查 zip 内容。
        pause
        exit /b 1
    )
    echo       Erlang 解压完成
)

REM ---- 解压 RabbitMQ ----
if exist "rabbitmq\sbin\rabbitmq-server.bat" (
    echo [跳过] RabbitMQ 已解压
) else (
    echo [2/2] 正在解压 RabbitMQ ...
    if exist "rabbitmq" rmdir /s /q "rabbitmq"
    mkdir "_rmq_tmp"
    if "%UNZIP%"=="tar" (
        tar -xf "rabbitmq.zip" -C "_rmq_tmp"
    ) else (
        powershell -NoProfile -Command "Expand-Archive -Path 'rabbitmq.zip' -DestinationPath '_rmq_tmp' -Force"
    )
    REM zip 内层有个 rabbitmq_server-4.3.6 目录，挪出来重命名为 rabbitmq
    for /d %%D in ("_rmq_tmp\rabbitmq_server-*") do move "%%D" "rabbitmq" >nul
    rmdir /s /q "_rmq_tmp" 2>nul
    if not exist "rabbitmq\sbin\rabbitmq-server.bat" (
        echo [错误] RabbitMQ 解压后结构不对。
        pause
        exit /b 1
    )
    echo       RabbitMQ 解压完成
)

echo.
echo ===================================================================
echo  准备完成！
echo.
echo  接下来：
echo    1) 双击 start-rabbitmq.bat     启动服务（保持窗口开着）
echo    2) 另开一个窗口，双击 enable-management.bat  开启管理台
echo    3) 浏览器打开 http://localhost:15672  (guest / guest)
echo ===================================================================
pause
endlocal
