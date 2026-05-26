import os
import asyncio
import json
from starlette.applications import Starlette
from starlette.websockets import WebSocket
from starlette.responses import PlainTextResponse
import sys

# Получаем порт от Render (или используем 8000 для локального теста)
PORT = int(os.environ.get('PORT', 8000))

async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("WebSocket connected", file=sys.stderr)
    
    target_reader = None
    target_writer = None
    
    try:
        # 1. Ждем первое сообщение с настройками подключения
        # Ожидаем формат: {"host": "example.com", "port": 443}
        data = await websocket.receive_text()
        config = json.loads(data)
        
        host = config['host']
        port = int(config['port'])
        
        print(f"Connecting to {host}:{port}", file=sys.stderr)
        
        # 2. Подключаемся к цели
        target_reader, target_writer = await asyncio.open_connection(host, port)
        await websocket.send_text("CONNECTED")
        
        # 3. Запускаем пересылку данных в обе стороны
        async def relay_ws_to_target():
            try:
                while True:
                    # Получаем бинарные данные от клиента
                    data = await websocket.receive_bytes()
                    if not data:
                        break
                    target_writer.write(data)
                    await target_writer.drain()
            except Exception as e:
                print(f"WS->Target Error: {e}", file=sys.stderr)
            finally:
                if target_writer:
                    target_writer.close()

        async def relay_target_to_ws():
            try:
                while True:
                    data = await target_reader.read(4096)
                    if not data:
                        break
                    await websocket.send_bytes(data)
            except Exception as e:
                print(f"Target->WS Error: {e}", file=sys.stderr)
            finally:
                await websocket.close()

        # Запускаем оба направления параллельно
        await asyncio.gather(relay_ws_to_target(), relay_target_to_ws())

    except Exception as e:
        print(f"Connection Error: {e}", file=sys.stderr)
    finally:
        if target_writer:
            target_writer.close()
        try:
            await websocket.close()
        except:
            pass
        print("WebSocket closed", file=sys.stderr)

# Создаем приложение
app = Starlette(
    routes=[
        ('/vpn', websocket_endpoint),
    ]
)

# Для локального запуска можно раскомментировать, но на Render мы будем использовать uvicorn через командную строку
# if __name__ == '__main__':
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=PORT)
