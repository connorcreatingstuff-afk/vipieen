import os
import asyncio
import json
import ssl
from starlette.applications import Starlette
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket
from starlette.responses import PlainTextResponse
import sys

PORT = int(os.environ.get('PORT', 8000))

ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

async def homepage(request):
    return PlainTextResponse("Proxy Server is running.")

async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("[WS] Client connected", file=sys.stderr)
    
    target_reader = None
    target_writer = None
    
    try:
        # Ждем первое сообщение с конфигурацией
        raw_data = await websocket.receive_text()
        print(f"[WS] Received raw config: '{raw_data}'", file=sys.stderr)
        
        try:
            config = json.loads(raw_data)
            host = config['host']
            port = int(config['port'])
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"[WS] Invalid JSON config: {e}", file=sys.stderr)
            await websocket.send_text("ERROR: Invalid config")
            await websocket.close()
            return
            
        print(f"[WS] Connecting to {host}:{port}", file=sys.stderr)
        
        try:
            if port == 443:
                target_reader, target_writer = await asyncio.open_connection(host, port, ssl=ssl_context)
            else:
                target_reader, target_writer = await asyncio.open_connection(host, port)
            
            print(f"[WS] Target connected", file=sys.stderr)
            await websocket.send_text("CONNECTED")
            
        except Exception as e:
            print(f"[WS] Target connection failed: {e}", file=sys.stderr)
            await websocket.send_text(f"ERROR: {str(e)}")
            await websocket.close()
            return

        async def relay_ws_to_target():
            try:
                while True:
                    data = await websocket.receive_bytes()
                    if not data: break
                    target_writer.write(data)
                    await target_writer.drain()
            except Exception as e:
                print(f"[Relay W->T] Error: {e}", file=sys.stderr)
            finally:
                if target_writer: target_writer.close()

        async def relay_target_to_ws():
            try:
                while True:
                    data = await target_reader.read(4096)
                    if not data: break
                    await websocket.send_bytes(data)
            except Exception as e:
                print(f"[Relay T->W] Error: {e}", file=sys.stderr)
            finally:
                await websocket.close()

        await asyncio.gather(relay_ws_to_target(), relay_target_to_ws())

    except Exception as e:
        print(f"[WS] General error: {e}", file=sys.stderr)
    finally:
        if target_writer: target_writer.close()
        try: await websocket.close()
        except: pass
        print("[WS] Closed", file=sys.stderr)

app = Starlette(
    routes=[
        Route('/', homepage),
        WebSocketRoute('/vpn', websocket_endpoint),
    ]
)
