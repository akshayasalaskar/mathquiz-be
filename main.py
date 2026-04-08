"""FastAPI entrypoint: leaderboard API and global math quiz WebSocket."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from dotenv import load_dotenv

import game_service
import mongo_client
import redis_client
from websocket_manager import get_manager

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await redis_client.connect()
    await mongo_client.connect()
    await game_service.init_if_needed()
    yield
    await redis_client.disconnect()
    await mongo_client.disconnect()


app = FastAPI(title="Competitive Math Quiz", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/leaderboard")
async def get_leaderboard():
    top = await mongo_client.top_leaderboard(10)
    return {"top": top}


@app.websocket("/ws")
async def quiz_ws(
    websocket: WebSocket,
    username: str = Query(..., min_length=1, max_length=64),
):
    manager = get_manager()
    ok, err = await manager.try_accept(username, websocket)
    if not ok:
        await websocket.send_json({"type": "error", "message": err or "Username already taken"})
        await websocket.close(code=4000, reason="Username already taken")
        return

    try:
        q = await game_service.get_current_question_public()
        await websocket.send_json({"type": "question", **q})
        while True:
            try:
                data = await websocket.receive_json()
            except WebSocketDisconnect:
                break
            except Exception:
                # Only respond if the connection is still open.
                if websocket.client_state.name == "CONNECTED":
                    await websocket.send_json(
                        {"type": "error", "message": "Invalid JSON payload"}
                    )
                    continue
                break

            msg_type = data.get("type")
            if msg_type == "answer":
                ans = data.get("answer")
                if ans is None:
                    await manager.send_personal(
                        username, {"type": "error", "message": "Missing answer field"}
                    )
                    continue
                await game_service.process_answer(manager, username, str(ans))
            else:
                await manager.send_personal(
                    username,
                    {
                        "type": "error",
                        "message": "Unknown message type (use type: answer)",
                    },
                )
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(username)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
