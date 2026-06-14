import os
import json
import base64
import asyncio
import traceback

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import Response
from twilio.rest import Client
import websockets

load_dotenv()

app = FastAPI(title="Daily Communication Voice Agent")

# =========================
# ENV VALUES
# =========================

PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip().rstrip("/")

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "").strip()
MY_PHONE_NUMBER = os.getenv("MY_PHONE_NUMBER", "").strip()

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY", "").strip()
CRON_SECRET = os.getenv("CRON_SECRET", "change-this-secret").strip()

DEEPGRAM_AGENT_URL = "wss://agent.deepgram.com/v1/agent/converse"


# =========================
# HELPERS
# =========================

def require_env():
    missing = []

    if not PUBLIC_URL:
        missing.append("PUBLIC_URL")
    if not TWILIO_ACCOUNT_SID:
        missing.append("TWILIO_ACCOUNT_SID")
    if not TWILIO_AUTH_TOKEN:
        missing.append("TWILIO_AUTH_TOKEN")
    if not TWILIO_FROM_NUMBER:
        missing.append("TWILIO_FROM_NUMBER")
    if not MY_PHONE_NUMBER:
        missing.append("MY_PHONE_NUMBER")
    if not DEEPGRAM_API_KEY:
        missing.append("DEEPGRAM_API_KEY")

    if missing:
        raise RuntimeError("Missing .env values: " + ", ".join(missing))


def get_ws_public_url():
    if PUBLIC_URL.startswith("https://"):
        return PUBLIC_URL.replace("https://", "wss://", 1)

    if PUBLIC_URL.startswith("http://"):
        return PUBLIC_URL.replace("http://", "ws://", 1)

    raise RuntimeError("PUBLIC_URL must start with https:// or http://")


def mask_key(key: str):
    if not key:
        return "NO_KEY"
    if len(key) <= 10:
        return key[:2] + "***"
    return key[:6] + "..." + key[-4:]


# =========================
# ROUTES
# =========================

@app.get("/")
def health():
    return {
        "status": "ok",
        "message": "Communication voice agent backend running"
    }


@app.get("/call-me")
def call_me(secret: str = Query(...)):
    """
    Browser/cron calls this endpoint.
    This triggers Twilio to call your phone.
    """

    if secret != CRON_SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")

    require_env()

    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

    call = client.calls.create(
        to=MY_PHONE_NUMBER,
        from_=TWILIO_FROM_NUMBER,
        url=f"{PUBLIC_URL}/twilio/voice",
        method="POST"
    )

    print("Call started:", call.sid)

    return {
        "status": "call_started",
        "call_sid": call.sid,
        "to": MY_PHONE_NUMBER
    }


@app.api_route("/twilio/voice", methods=["GET", "POST"])
async def twilio_voice():
    """
    Twilio hits this endpoint after your phone answers.
    We return TwiML to connect the call to our WebSocket.
    """

    ws_url = get_ws_public_url()
    stream_url = f"{ws_url}/ws/twilio"

    twiml = f"""
<Response>
    <Say>Hi Vinoth. Starting your communication practice now.</Say>
    <Connect>
        <Stream url="{stream_url}" />
    </Connect>
</Response>
"""

    print("Twilio Stream URL:", stream_url)

    return Response(content=twiml.strip(), media_type="application/xml")


# =========================
# DEEPGRAM
# =========================

async def open_deepgram_socket():
    """
    Connect to Deepgram Voice Agent.

    First tries Authorization header auth.
    If that fails, tries Deepgram Twilio guide's subprotocol auth.
    """

    print("Connecting to Deepgram Voice Agent...")
    print("Deepgram key loaded:", mask_key(DEEPGRAM_API_KEY))

    # Method 1: Authorization header auth
    try:
        print("Trying Deepgram auth method: Authorization header")

        return await websockets.connect(
            DEEPGRAM_AGENT_URL,
            additional_headers={
                "Authorization": f"Token {DEEPGRAM_API_KEY}"
            },
            ping_interval=20,
            ping_timeout=20,
        )

    except Exception as first_error:
        print("Header auth failed:", repr(first_error))
        print("Trying Deepgram auth method: subprotocol token")

    # Method 2: Subprotocol auth fallback
    return await websockets.connect(
        DEEPGRAM_AGENT_URL,
        subprotocols=["token", DEEPGRAM_API_KEY],
        ping_interval=20,
        ping_timeout=20,
    )


async def connect_deepgram():
    """
    Connect to Deepgram and send Voice Agent settings.
    """

    dg_ws = await open_deepgram_socket()

    settings = {
        "type": "Settings",
        "audio": {
            "input": {
                "encoding": "mulaw",
                "sample_rate": 8000
            },
            "output": {
                "encoding": "mulaw",
                "sample_rate": 8000,
                "container": "none"
            }
        },
        "agent": {
            "language": "en",
            "listen": {
                "provider": {
                    "type": "deepgram",
                    "model": "nova-3",
                    "smart_format": False
                }
            },
            "think": {
                "provider": {
                    "type": "open_ai",
                    "model": "gpt-4o-mini",
                    "temperature": 0.7
                },
                "prompt": """
You are Vinoth's personal communication coach.

Your goal:
Help Vinoth improve English communication, business speaking,
meeting confidence, and sales/client conversations.

Conversation rules:
- Ask only one question at a time.
- Keep replies short and simple.
- Wait for Vinoth's answer.
- After he answers, give short feedback.
- Correct grammar politely.
- Give one better sentence example.
- Then ask the next question.
- Do not give long lectures.

Daily practice flow:
1. Ask how his day is going.
2. Ask him to introduce himself in 30 seconds.
3. Ask one business communication question.
4. Ask one client/sales conversation question.
5. Give a score out of 10.
6. Give 3 improvements for tomorrow.
"""
            },
            "speak": {
                "provider": {
                    "type": "deepgram",
                    "model": "aura-2-thalia-en"
                }
            },
            "greeting": "Hi Vijay, I am your communication coach. Let us start. How is your day going today?"
        }
    }

    await dg_ws.send(json.dumps(settings))
    print("Deepgram settings sent")

    return dg_ws


# =========================
# TWILIO WEBSOCKET BRIDGE
# =========================

@app.websocket("/ws/twilio")
async def twilio_websocket(websocket: WebSocket):
    """
    Bridge:
    Twilio phone call audio <-> FastAPI <-> Deepgram Voice Agent
    """

    await websocket.accept()
    print("Twilio WebSocket connected")

    audio_queue = asyncio.Queue()
    streamsid_queue = asyncio.Queue()
    dg_ws = None

    try:
        dg_ws = await connect_deepgram()
        print("Deepgram connected")

        async def twilio_receiver():
            """
            Receive audio from Twilio and put it into queue for Deepgram.
            """

            print("twilio_receiver started")

            # Twilio sends very small audio chunks.
            # Deepgram guide uses raw mulaw 8000 Hz audio for Twilio.
            # Twilio/Deepgram telephony integration expects mulaw 8000.
            BUFFER_SIZE = 20 * 160
            inbuffer = bytearray()

            while True:
                message = await websocket.receive_text()
                data = json.loads(message)

                event = data.get("event")

                if event == "connected":
                    print("Twilio event: connected")

                elif event == "start":
                    stream_sid = data["start"]["streamSid"]
                    print("Twilio stream started:", stream_sid)
                    await streamsid_queue.put(stream_sid)

                elif event == "media":
                    media = data.get("media", {})
                    payload = media.get("payload")

                    if not payload:
                        continue

                    chunk = base64.b64decode(payload)

                    # Accept inbound track or no track field
                    track = media.get("track")
                    if track is None or track == "inbound":
                        inbuffer.extend(chunk)

                    while len(inbuffer) >= BUFFER_SIZE:
                        audio_chunk = bytes(inbuffer[:BUFFER_SIZE])
                        await audio_queue.put(audio_chunk)
                        inbuffer = inbuffer[BUFFER_SIZE:]

                elif event == "stop":
                    print("Twilio stream stopped")
                    await audio_queue.put(None)
                    break

                else:
                    print("Unknown Twilio event:", event)

        async def deepgram_sender():
            """
            Send caller audio from queue to Deepgram.
            """

            print("deepgram_sender started")

            while True:
                chunk = await audio_queue.get()

                if chunk is None:
                    print("deepgram_sender stopping")
                    break

                await dg_ws.send(chunk)

        async def deepgram_receiver():
            """
            Receive Deepgram AI voice/audio and send back to Twilio.
            """

            print("deepgram_receiver started")

            stream_sid = await streamsid_queue.get()
            print("Using stream_sid:", stream_sid)

            async for message in dg_ws:
                if isinstance(message, str):
                    try:
                        event = json.loads(message)
                        event_type = event.get("type")

                        print("Deepgram event:", event_type)

                        if event_type == "Error":
                            print("Deepgram ERROR:", json.dumps(event, indent=2))

                        elif event_type == "Warning":
                            print("Deepgram WARNING:", json.dumps(event, indent=2))

                        elif event_type == "ConversationText":
                            role = event.get("role")
                            content = event.get("content")
                            print(f"{role}: {content}")

                        elif event_type == "UserStartedSpeaking":
                            # Barge-in: clear current agent audio if user starts talking
                            clear_message = {
                                "event": "clear",
                                "streamSid": stream_sid
                            }
                            await websocket.send_text(json.dumps(clear_message))

                    except Exception:
                        print("Deepgram text message:", message)

                else:
                    # Deepgram returns raw mulaw audio bytes.
                    # Twilio needs base64 payload in a media message.
                    audio_payload = base64.b64encode(message).decode("ascii")

                    twilio_audio = {
                        "event": "media",
                        "streamSid": stream_sid,
                        "media": {
                            "payload": audio_payload
                        }
                    }

                    await websocket.send_text(json.dumps(twilio_audio))

        tasks = [
            asyncio.create_task(twilio_receiver()),
            asyncio.create_task(deepgram_sender()),
            asyncio.create_task(deepgram_receiver()),
        ]

        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            if task.exception():
                print("Task error:", repr(task.exception()))

        for task in pending:
            task.cancel()

    except WebSocketDisconnect:
        print("Twilio WebSocket disconnected")

    except Exception as e:
        print("MAIN ERROR:", str(e))
        traceback.print_exc()

    finally:
        if dg_ws:
            try:
                await dg_ws.close()
            except Exception:
                pass

        print("Call bridge closed")