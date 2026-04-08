"""Question generation, Redis game state, rate limits, and win handling."""

import random
from typing import Any

import mongo_client
from redis_client import redis_conn
from websocket_manager import ConnectionManager

KEY_QUESTION = "current_question"
KEY_ANSWER = "correct_answer"
KEY_WINNER_LOCK = "winner_lock"


def generate_question() -> tuple[str, str]:
    """Random addition or multiplication (1–20 operands)."""
    a = random.randint(1, 20)
    b = random.randint(1, 20)
    if random.choice((True, False)):
        text = f"What is {a} + {b}?"
        return text, str(a + b)
    text = f"What is {a} * {b}?"
    return text, str(a * b)


def _rate_key(username: str) -> str:
    return f"rate:{username}"


async def check_rate_limit(username: str) -> bool:
    """Return True if request is allowed (<= 5 answers in the last 1s window)."""
    r = redis_conn()
    key = _rate_key(username)
    count = await r.incr(key)
    if count == 1:
        await r.expire(key, 1)
    return count <= 5


async def _write_new_round_to_redis(question_text: str, answer: str) -> None:
    r = redis_conn()
    pipe = r.pipeline(transaction=True)
    pipe.delete(KEY_WINNER_LOCK)
    pipe.set(KEY_QUESTION, question_text)
    pipe.set(KEY_ANSWER, answer)
    await pipe.execute()


async def init_if_needed() -> None:
    r = redis_conn()
    if not await r.exists(KEY_QUESTION):
        text, ans = generate_question()
        await _write_new_round_to_redis(text, ans)


async def get_current_question_public() -> dict[str, Any]:
    r = redis_conn()
    text = await r.get(KEY_QUESTION)
    if text is None:
        text, ans = generate_question()
        await _write_new_round_to_redis(text, ans)
        text = await r.get(KEY_QUESTION)
    return {"text": text}


async def new_round_after_win(manager: ConnectionManager) -> None:
    text, ans = generate_question()
    await _write_new_round_to_redis(text, ans)
    await manager.broadcast_json({"type": "question", "text": text})


def _normalize_answer(raw: str) -> str:
    s = raw.strip()
    try:
        return str(int(s))
    except ValueError:
        return s


async def process_answer(
    manager: ConnectionManager,
    username: str,
    answer_raw: str,
) -> None:
    """
    Validates rate limit and answer, uses Redis NX lock for single winner per round.
    Sends personal messages; broadcasts winner + new question + leaderboard on win.
    """
    if not await check_rate_limit(username):
        await manager.send_personal(
            username,
            {"type": "error", "message": "Rate limit exceeded (max 5 answers per second)"},
        )
        return

    r = redis_conn()
    correct_str = await r.get(KEY_ANSWER)
    if correct_str is None:
        await manager.send_personal(
            username, {"type": "error", "message": "No active question"}
        )
        return

    user_ans = _normalize_answer(answer_raw)
    if user_ans != correct_str:
        await manager.send_personal(
            username,
            {
                "type": "result",
                "correct": False,
                "you_won": False,
            },
        )
        return

    # Correct: race for winner_lock (SET NX)
    acquired = await r.set(KEY_WINNER_LOCK, username, nx=True)
    if not acquired:
        await manager.send_personal(
            username,
            {
                "type": "result",
                "correct": True,
                "you_won": False,
            },
        )
        return

    # This connection won the race (single winner enforced by Redis SET NX)
    await manager.broadcast_json({"type": "winner", "winner_username": username})

    await mongo_client.increment_score(username, 10)
    top = await mongo_client.top_leaderboard(10)
    await manager.broadcast_json({"type": "leaderboard", "top": top})

    await new_round_after_win(manager)

    await manager.send_personal(
        username,
        {"type": "result", "correct": True, "you_won": True},
    )
