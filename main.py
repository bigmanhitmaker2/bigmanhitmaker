from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import json
import asyncio
import logging
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import os
from pydantic import BaseModel
import redis.asyncio as redis
import hashlib
import time
import re
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from fastapi import Security, HTTPException, status

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
API_URL = os.getenv("API_BASE_URL") or os.getenv("SUNO_API_BASE_URL", "https://apibox.erweima.ai/api/v1/generate")
API_KEY = os.getenv("API_TOKEN") or os.getenv("SUNO_API_KEY")
REDIS_URL = os.getenv("REDIS_URL")
DEFAULT_CALLBACK_URL = os.getenv("CALLBACK_URL", "https://www.bigmanhitmaker.com/callback")
if not API_KEY:
    logger.warning("API_TOKEN/SUNO_API_KEY not set. API calls may fail.")

# Initialize Redis client with retry mechanism
async def init_redis_client():
    max_retries = 5
    retry_delay = 2
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        logger.error("REDIS_URL environment variable not set")
        logger.warning("Using in-memory fallback due to missing REDIS_URL")
        return None
    for attempt in range(max_retries):
        try:
            redis_client = redis.Redis.from_url(redis_url, decode_responses=True, ssl_cert_reqs=None)
            await redis_client.ping()
            logger.info("Redis client initialized successfully with Upstash")
            return redis_client
        except Exception as e:
            logger.error(f"Failed to initialize Redis (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
    logger.warning("Redis initialization failed, using in-memory fallback")
    return None

redis_client = None
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    try:
        if request.method == "POST":
            body = await request.body()
            logger.info(f"Request body: {body.decode('utf-8', errors='ignore')}")
    except Exception as e:
        logger.error(f"Failed to log request body: {str(e)}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code}")
    return response

# Initialize Redis and limiter on startup
@app.on_event("startup")
async def startup_event():
    global redis_client, tasks
    redis_client = await init_redis_client()
    if redis_client is None:
        tasks = {}  # In-memory fallback
    else:
        await FastAPILimiter.init(redis_client)  # Init limiter with Redis
        try:
            await redis_client.config_set("notify-keyspace-events", "KEA")
            logger.info("Redis keyspace notifications enabled")
        except Exception as e:
            logger.error(f"Failed to enable Redis keyspace notifications: {str(e)}")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")  # For bearer tokens

# Token validation function
async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        jwks_url = f"https://{os.getenv('AUTH0_DOMAIN')}/.well-known/jwks.json"
        # Fetch JWKS (cache in production)
        async with httpx.AsyncClient() as client:
            jwks_response = await client.get(jwks_url)
            jwks = jwks_response.json()
        # Decode and validate JWT
        header = jwt.get_unverified_header(token)
        rsa_key = next((k for k in jwks["keys"] if k["kid"] == header["kid"]), None)
        if not rsa_key:
            raise credentials_exception
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            audience=os.getenv("AUTH0_AUDIENCE"),
            issuer=f"https://{os.getenv('AUTH0_DOMAIN')}/",
        )
        return payload  # User info from token
    except JWTError:
        raise credentials_exception

# Mount static files
app.mount("/static", StaticFiles(directory="/app/static"), name="static")

# Serve favicon.ico
@app.get("/favicon.ico")
async def favicon():
    favicon_path = "/app/static/favicon.ico"
    if not os.path.exists(favicon_path):
        logger.error(f"Favicon not found at {favicon_path}")
        raise HTTPException(status_code=404, detail="Favicon not found")
    logger.info(f"Serving favicon from {favicon_path}")
    return FileResponse(favicon_path, media_type="image/x-icon")

# Serve index.html
@app.get("/")
async def serve_index():
    index_path = "/app/static/index.html"
    if not os.path.exists(index_path):
        logger.error(f"Index file not found at {index_path}")
        raise HTTPException(status_code=404, detail="Index file not found")
    logger.info(f"Serving index.html from {index_path}")
    return FileResponse(index_path, headers={"Cache-Control": "no-cache, no-store, must-revalidate"})

# Handle HEAD request for root
@app.head("/")
async def head_index():
    logger.info("Handling HEAD request for /")
    return Response(headers={"Cache-Control": "no-cache, no-store, must-revalidate"})

# Analytics endpoint
@app.post("/analytics")
async def analytics(request: Request):
    try:
        data = await request.json()
        event = data.get("event")
        event_data = data.get("data", {})
        logger.info(f"Analytics event: {event}, data: {json.dumps(event_data, indent=2)}")
        if redis_client:
            async with redis_client.pipeline(transaction=True) as pipe:
                await pipe.hset(f"analytics:events:{event}", mapping={
                    "timestamp": str(int(asyncio.get_event_loop().time())),
                    "data": json.dumps(event_data)
                })
                await pipe.execute()
        return {"msg": "success", "message": "Analytics event recorded"}
    except Exception as e:
        logger.error(f"Analytics error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Invalid analytics data: {str(e)}")

# Pydantic models
class SongRequest(BaseModel):
    prompt: str
    style: str = ""
    title: str = ""
    custom_mode: bool = False
    instrumental: bool = False
    model: str = "V4_5"
    instruments: Optional[List[str]] = None
    language: Optional[str] = None
    negative_tags: Optional[List[str]] = None
    duration: Optional[str] = None
    lyrics: Optional[str] = None
    mood: Optional[str] = None
    callBackUrl: Optional[str] = None

class CallbackTrack(BaseModel):
    audio_url: Optional[str] = ""
    stream_audio_url: Optional[str] = ""
    source_audio_url: Optional[str] = ""
    source_stream_audio_url: Optional[str] = ""
    image_url: Optional[str] = ""
    title: Optional[str] = ""
    prompt: Optional[str] = ""
    tags: Optional[str] = ""
    id: Optional[str] = ""
    duration: Optional[float] = 0.0
    createTime: Optional[int] = None
    model_name: Optional[str] = None
    lyrics: Optional[str] = None
    model_config = {"protected_namespaces": ()}

class CallbackInnerData(BaseModel):
    callbackType: str
    data: List[CallbackTrack]
    task_id: str

class FullCallbackPayload(BaseModel):
    code: int
    msg: str
    data: CallbackInnerData

# Validate audio URL with caching
async def validate_audio_url(url: str) -> bool:
    if not url:
        return False
    cache_key = f"audio_url:{hashlib.md5(url.encode()).hexdigest()}"
    if redis_client:
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                logger.info(f"Returning cached validation for audio URL: {url}")
                return True
        except Exception as e:
            logger.error(f"Redis error in validate_audio_url: {str(e)}")
    max_retries = 2
    retry_delay = 1
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            async with httpx.AsyncClient() as client:
                response = await client.head(url, timeout=5.0)
                logger.info(f"Audio URL validation (HEAD) for {url}: status={response.status_code}, duration={time.time() - start_time:.2f}s")
                if response.status_code == 200:
                    if redis_client:
                        try:
                            await redis_client.set(cache_key, "true", ex=3600)
                        except Exception as e:
                            logger.error(f"Redis error caching audio URL: {str(e)}")
                    return True
                if response.status_code == 405:
                    logger.warning(f"HEAD request not allowed for {url}, trying GET")
                    response = await client.get(url, timeout=5.0)
                    logger.info(f"Audio URL validation (GET) for {url}: status={response.status_code}, duration={time.time() - start_time:.2f}s")
                    if response.status_code == 200 and redis_client:
                        try:
                            await redis_client.set(cache_key, "true", ex=3600)
                        except Exception as e:
                            logger.error(f"Redis error caching audio URL: {str(e)}")
                    return response.status_code == 200
                return False
        except Exception as e:
            logger.error(f"Failed to validate audio URL {url} (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
            else:
                return False
    return False

# Background audio validation coroutine
async def validate_and_update_audio(task_id: str, tracks: List[Dict], original_prompt: Optional[str], original_lyrics: Optional[str]):
    if not tracks:
        logger.warning(f"No tracks to validate for task_id: {task_id}")
        task_data = {
            "status": "pending",
            "tracks": [],
            "progress": 30,
            "original_prompt": original_prompt,
            "original_lyrics": original_lyrics,
            "created_at": int(time.time())
        }
        if redis_client:
            try:
                async with redis_client.pipeline(transaction=True) as pipe:
                    await pipe.set(f"task:{task_id}", json.dumps(task_data))
                    await pipe.publish(f"channel:{task_id}", json.dumps(task_data))
                    await pipe.execute()
                logger.info(f"Background validation updated task_id: {task_id} with no tracks")
            except Exception as e:
                logger.error(f"Failed to update Redis in background validation: {str(e)}")
        elif tasks is not None:
            tasks[task_id] = task_data
        return
    validated_tracks = []
    for track in tracks:
        audio_url = track.get("source_stream_audio_url") or track.get("source_audio_url") or track.get("stream_audio_url") or track.get("audio_url") or ""
        is_valid = await validate_audio_url(audio_url)
        validated_track = {
            "id": track["id"],
            "title": track["title"] or "Untitled",
            "audio_url": audio_url if is_valid else "",
            "image_url": track.get("image_url") or track.get("source_image_url") or "",
            "prompt": track.get("prompt") or original_prompt or "",
            "has_audio": is_valid,
            "lyrics": track.get("lyrics") or track.get("prompt") or original_lyrics or "",
            "tags": track.get("tags") or ""
        }
        validated_tracks.append(validated_track)
        logger.info(f"Validated track {track['id']} for task_id: {task_id}, has_audio: {is_valid}, audio_url: {audio_url}")
    task_data = {
        "status": "done" if validated_tracks and all(t["has_audio"] for t in validated_tracks) else "pending",
        "tracks": validated_tracks,
        "progress": 100 if validated_tracks and all(t["has_audio"] for t in validated_tracks) else tracks[0].get("progress", 30),
        "original_prompt": original_prompt,
        "original_lyrics": original_lyrics,
        "created_at": int(time.time())
    }
    if redis_client:
        try:
            async with redis_client.pipeline(transaction=True) as pipe:
                await pipe.set(f"task:{task_id}", json.dumps(task_data))
                await pipe.publish(f"channel:{task_id}", json.dumps(task_data))
                await pipe.execute()
            logger.info(f"Background validation updated task_id: {task_id}, data: {json.dumps(task_data, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to update Redis in background validation: {str(e)}")
    elif tasks is not None:
        tasks[task_id] = task_data

# Validate callback URL
def validate_callback_url(url: str) -> bool:
    if not url:
        return False
    url_pattern = re.compile(r'^https?://[\w\-\.]+(:[0-9]+)?(/[\w\-\./]*)*$')
    is_valid = bool(url_pattern.match(url))
    logger.info(f"Validating callBackUrl: {url}, is_valid: {is_valid}")
    return is_valid

# Poll task status
async def poll_task_status(task_id: str):
    max_attempts = 180  # Poll for 15 minutes (180 * 5s = 900s)
    poll_interval = 5
    logger.info(f"Starting poll_task_status for task_id: {task_id}")
    initial_task_data = None
    if redis_client:
        try:
            initial_task_data = await redis_client.get(f"task:{task_id}")
            initial_task_data = json.loads(initial_task_data) if initial_task_data else {}
        except Exception as e:
            logger.error(f"Failed to fetch initial task data for task_id: {task_id}: {str(e)}")
    elif tasks is not None:
        initial_task_data = tasks.get(f"task:{task_id}", {})
    for attempt in range(max_attempts):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_attempts} for task_id: {task_id}")
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{API_URL}/record-info?taskId={task_id}",
                    headers={"Authorization": f"Bearer {API_KEY}"},
                    timeout=900.0
                )
                logger.info(f"HTTP response status for task_id {task_id}: {response.status_code}")
                response.raise_for_status()
                result = response.json()
                logger.info(f"Polled status for task_id: {task_id}, response: {json.dumps(result, indent=2)}")
                if result.get("code") == 200 and result.get("data"):
                    param = result["data"].get("param", "{}")
                    prompt_from_param = None
                    try:
                        param_data = json.loads(param)
                        prompt_from_param = param_data.get("prompt")
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse param for task_id: {task_id}")
                    suno_data = result["data"].get("response", {}).get("sunoData", [{}])
                    lyrics_from_response = suno_data[0].get("prompt") if suno_data and not initial_task_data.get("instrumental") else None
                    task_data = {
                        "status": result["data"].get("status", "pending"),
                        "tracks": result["data"].get("response", {}).get("sunoData", []),
                        "progress": result["data"].get("progress", 0),
                        "original_prompt": initial_task_data.get("original_prompt", prompt_from_param),
                        "original_lyrics": initial_task_data.get("original_lyrics", lyrics_from_response),
                        "created_at": int(time.time()),
                        "style": initial_task_data.get("style", ""),
                        "title": initial_task_data.get("title", ""),
                        "custom_mode": initial_task_data.get("custom_mode", False),
                        "instrumental": initial_task_data.get("instrumental", False),
                        "model": initial_task_data.get("model", "V4_5"),
                        "instruments": initial_task_data.get("instruments", None),
                        "language": initial_task_data.get("language", None),
                        "negative_tags": initial_task_data.get("negative_tags", None),
                        "duration": initial_task_data.get("duration", None),
                        "mood": initial_task_data.get("mood", None),
                        "callBackUrl": initial_task_data.get("callBackUrl", DEFAULT_CALLBACK_URL)
                    }
                    if redis_client:
                        try:
                            async with redis_client.pipeline(transaction=True) as pipe:
                                await pipe.set(f"task:{task_id}", json.dumps(task_data))
                                await pipe.publish(f"channel:{task_id}", json.dumps(task_data))
                                await pipe.execute()
                        except Exception as e:
                            logger.error(f"Redis error storing task data in poll_task_status: {str(e)}")
                    elif tasks is not None:
                        tasks[f"task:{task_id}"] = task_data
                    if task_data["status"] in ["SUCCESS", "FAILED"]:
                        logger.info(f"Task {task_id} reached final status: {task_data['status']}")
                        if task_data["status"] == "SUCCESS":
                            asyncio.create_task(validate_and_update_audio(task_id, task_data["tracks"], task_data["original_prompt"], task_data["original_lyrics"]))
                        return result
        except Exception as e:
            logger.error(f"Failed to poll status for task_id: {task_id}, attempt {attempt + 1}/{max_attempts}: {str(e)}")
        await asyncio.sleep(poll_interval)
    logger.error(f"Polling timeout for task_id: {task_id}")
    task_data = {
        "status": "failed",
        "tracks": [],
        "progress": 0,
        "original_prompt": initial_task_data.get("original_prompt"),
        "original_lyrics": initial_task_data.get("original_lyrics"),
        "created_at": int(time.time())
    }
    if redis_client:
        try:
            async with redis_client.pipeline(transaction=True) as pipe:
                await pipe.set(f"task:{task_id}", json.dumps(task_data))
                await pipe.publish(f"channel:{task_id}", json.dumps(task_data))
                await pipe.execute()
        except Exception as e:
            logger.error(f"Redis error storing timeout data: {str(e)}")
    elif tasks is not None:
        tasks[f"task:{task_id}"] = task_data
    return {"code": 500, "msg": "Polling timeout", "data": {"taskId": task_id, "status": "FAILED"}}

# Timeout check for callback
async def check_callback_timeout(task_id: str):
    await asyncio.sleep(900)  # Wait 15 minutes
    if redis_client:
        try:
            callback_received = await redis_client.get(f"callback_received:{task_id}")
            task_data = await redis_client.get(f"task:{task_id}")
            if callback_received == "false" and task_data:
                task_data = json.loads(task_data)
                if not task_data["tracks"]:
                    task_data["status"] = "failed"
                    task_data["progress"] = 0
                    async with redis_client.pipeline(transaction=True) as pipe:
                        await pipe.set(f"task:{task_id}", json.dumps(task_data))
                        await pipe.publish(f"channel:{task_id}", json.dumps(task_data))
                        await pipe.execute()
                    logger.error(f"Callback timeout for task_id: {task_id}, marked as failed")
        except Exception as e:
            logger.error(f"Error checking callback timeout for task_id: {task_id}: {str(e)}")
    elif tasks is not None:
        task_data = tasks.get(f"task:{task_id}")
        if task_data and not task_data["tracks"]:
            task_data["status"] = "failed"
            task_data["progress"] = 0
            tasks[f"task:{task_id}"] = task_data
            logger.error(f"Callback timeout for task_id: {task_id}, marked as failed")

# Song generation API
async def generate_song(
    prompt: str,
    style: str,
    title: str,
    custom_mode: bool,
    instrumental: bool,
    model: str,
    instruments: Optional[List[str]] = None,
    language: Optional[str] = None,
    negative_tags: Optional[List[str]] = None,
    duration: Optional[str] = None,
    lyrics: Optional[str] = None,
    mood: Optional[str] = None,
    callBackUrl: Optional[str] = None,
    retry_count: int = 0
) -> Dict[str, Any]:
    logger.info("Using API URL: %s", API_URL)
    effective_callback_url = callBackUrl if callBackUrl and validate_callback_url(callBackUrl) else DEFAULT_CALLBACK_URL
    if not effective_callback_url:
        logger.error("No valid callBackUrl provided")
        return {"msg": "error", "message": "No valid callBackUrl provided"}
    logger.info(f"Using callBackUrl: {effective_callback_url}")

    if not prompt or len(prompt) < 5:
        logger.error(f"Invalid prompt: {prompt}")
        return {"msg": "error", "message": "Prompt is required (min 5, max 295 characters)"}
    if len(prompt) > 295:
        logger.warning(f"Prompt exceeds 295 characters, trimming to fit")
        prompt = prompt[:295]

    if custom_mode and not instrumental:
        if not lyrics or len(lyrics) < 5:
            logger.error("Lyrics required in custom mode when not instrumental")
            return {"msg": "error", "message": "Lyrics required for vocal tracks (min 5, max 3000 characters)"}
        if len(lyrics) > 3000:
            logger.warning(f"Lyrics exceed 3000 characters, trimming to fit")
            lyrics = lyrics[:3000]

    prompt_hash = hashlib.md5((prompt + (lyrics or "")).encode()).hexdigest()
    cache_key = f"prompt:{prompt_hash}"
    if redis_client:
        try:
            cached_result = await redis_client.get(cache_key)
            if cached_result:
                logger.info(f"Returning cached result for prompt hash: {prompt_hash}")
                cached_data = json.loads(cached_result)
                task_id = cached_data["data"]["taskId"]
                task_data = await redis_client.get(f"task:{task_id}")
                if not task_data:
                    logger.warning(f"Task {task_id} not found in Redis, clearing cache")
                    await redis_client.delete(cache_key)
                else:
                    return cached_data
        except Exception as e:
            logger.error(f"Redis error checking cache: {str(e)}")

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "prompt": lyrics if custom_mode and not instrumental else prompt,
        "customMode": custom_mode,
        "instrumental": instrumental,
        "model": model or "V4_5",
        "callBackUrl": effective_callback_url
    }
    if custom_mode:
        payload["style"] = style
        payload["title"] = title
        if mood:
            payload["tags"] = mood
    if instruments:
        payload["instruments"] = instruments
    if language:
        payload["language"] = language
    if negative_tags:
        payload["negative_tags"] = ",".join(negative_tags)
    if duration:
        payload["duration"] = duration
    elif custom_mode and not instrumental:
        payload["duration"] = "240"
    else:
        payload["duration"] = "180"

    max_retries = 3
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            logger.info("Step 1/4 - Sending request to API (attempt %s/%s, retry_count=%s): headers=%s, payload=%s",
                        attempt + 1, max_retries, retry_count, json.dumps(dict(headers), indent=2), json.dumps(payload, indent=2))
            start_time = time.time()
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    API_URL,
                    headers=headers,
                    json=payload,
                    timeout=30.0
                )
                logger.info(f"Step 2/4 - API response received (duration: {time.time() - start_time:.2f}s): status={response.status_code}, headers={json.dumps(dict(response.headers), indent=2)}, body={response.text}")
                response.raise_for_status()
                try:
                    result = response.json()
                    if result.get("code") != 200:
                        logger.error(f"API returned non-200 code: {result.get('code')}, message: {result.get('msg')}")
                        return {"msg": "error", "message": f"API error: {result.get('msg', 'Unknown error')}"}
                    if not result.get("data") or "taskId" not in result["data"]:
                        logger.error(f"Invalid API response structure: {response.text}")
                        return {"msg": "error", "message": "Invalid API response structure: missing data or taskId"}
                    task_id = result["data"]["taskId"]
                    logger.info(f"Step 3/4 - Task ID received from API: {task_id}")
                    task_data = {
                        "status": "pending",
                        "tracks": [],
                        "progress": 0,
                        "original_prompt": prompt,
                        "original_lyrics": lyrics if custom_mode and not instrumental else None,
                        "created_at": int(time.time()),
                        "retry_count": retry_count,
                        "style": style,
                        "title": title,
                        "custom_mode": custom_mode,
                        "instrumental": instrumental,
                        "model": model or "V4_5",
                        "instruments": instruments,
                        "language": language,
                        "negative_tags": negative_tags,
                        "duration": duration,
                        "mood": mood,
                        "callBackUrl": effective_callback_url
                    }
                    if redis_client:
                        try:
                            async with redis_client.pipeline(transaction=True) as pipe:
                                await pipe.set(f"task:{task_id}", json.dumps(task_data))
                                await pipe.set(f"callback_received:{task_id}", "false")
                                await pipe.publish(f"channel:{task_id}", json.dumps(task_data))
                                await pipe.set(f"prompt:{prompt_hash}", json.dumps({"msg": "success", "data": {"taskId": task_id}}))
                                await pipe.execute()
                            logger.info(f"Step 4/4 - Task data stored in Redis for task_id: {task_id}, data: {json.dumps(task_data, indent=2)}")
                            asyncio.create_task(poll_task_status(task_id))
                            asyncio.create_task(check_callback_timeout(task_id))
                        except Exception as e:
                            logger.error(f"Failed to store task data in Redis: {str(e)}")
                            return {"msg": "error", "message": f"Redis error: {str(e)}"}
                    elif tasks is not None:
                        tasks[task_id] = task_data
                        tasks[f"prompt:{prompt_hash}"] = {"msg": "success", "data": {"taskId": task_id}}
                        asyncio.create_task(poll_task_status(task_id))
                        asyncio.create_task(check_callback_timeout(task_id))
                    return result
                except ValueError as e:
                    logger.error(f"Failed to parse API response as JSON: {response.text}")
                    return {"msg": "error", "message": "Invalid API response format"}
                except Exception as e:
                    logger.error(f"Unexpected error parsing API response: {str(e)}")
                    return {"msg": "error", "message": f"Unexpected error: {str(e)}"}
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred (attempt {attempt + 1}/{max_retries}): status={e.response.status_code}, message={str(e)}")
            if e.response.status_code in (401, 403):
                return {"msg": "error", "message": "Authentication error: Invalid or expired API key"}
            elif e.response.status_code == 429:
                return {"msg": "error", "message": "Rate limit exceeded, please try again later"}
            elif e.response.status_code == 404:
                return {"msg": "error", "message": f"Endpoint not found: {API_URL}"}
            elif e.response.status_code == 400:
                try:
                    error_data = e.response.json()
                    if "callBackUrl" in error_data.get("message", "").lower():
                        return {"msg": "error", "message": "Missing or invalid callBackUrl in request payload"}
                    return {"msg": "error", "message": f"Bad request: {error_data.get('msg', e.response.text)}"}
                except ValueError:
                    return {"msg": "error", "message": f"Bad request: {e.response.text}"}
            elif e.response.status_code == 503 and attempt < max_retries - 1:
                logger.info(f"Retrying after {retry_delay} seconds due to 503 error")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 10)
                continue
            return {"msg": "error", "message": f"HTTP error: {str(e)}"}
        except httpx.RequestError as e:
            logger.error(f"Network error occurred (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying after {retry_delay} seconds due to network error")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 10)
                continue
            return {"msg": "error", "message": f"Network error: {str(e)}"}
        except Exception as e:
            logger.error(f"Unexpected error in generate_song (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying after {retry_delay} seconds due to unexpected error")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 10)
                continue
            return {"msg": "error", "message": f"Unexpected error: {str(e)}"}

# Retry task endpoint
@app.post("/retry/{task_id}")
async def retry_task(task_id: str, user: dict = Depends(get_current_user)):
    task_data = None
    if redis_client:
        try:
            task_data = await redis_client.get(f"task:{task_id}")
        except Exception as e:
            logger.error(f"Redis error fetching task {task_id}: {str(e)}")
    elif tasks is not None:
        task_data = tasks.get(f"task:{task_id}")

    if not task_data:
        logger.error(f"Task not found for retry: {task_id}")
        raise HTTPException(status_code=404, detail="Task not found")

    task_data = json.loads(task_data) if redis_client else task_data
    if task_data["status"] != "failed":
        logger.warning(f"Task {task_id} is not in failed state, cannot retry")
        return {"msg": "error", "message": "Task is not in failed state"}

    retry_count = task_data.get("retry_count", 0) + 1
    if retry_count > 2:
        logger.error(f"Max retries reached for task_id: {task_id}")
        return {"msg": "error", "message": "Maximum retry attempts reached"}

    logger.info(f"Retrying task_id: {task_id}, retry_count: {retry_count}")
    result = await generate_song(
        prompt=task_data["original_prompt"],
        style=task_data.get("style", ""),
        title=task_data.get("title", ""),
        custom_mode=task_data.get("custom_mode", False),
        instrumental=task_data.get("instrumental", False),
        model=task_data.get("model", "V4_5"),
        instruments=task_data.get("instruments", None),
        language=task_data.get("language", None),
        negative_tags=task_data.get("negative_tags", None),
        duration=task_data.get("duration", None),
        lyrics=task_data.get("original_lyrics", None),
        mood=task_data.get("mood", None),
        callBackUrl=task_data.get("callBackUrl", DEFAULT_CALLBACK_URL),
        retry_count=retry_count
    )

    if result.get("msg") == "success":
        task_data["status"] = "pending"
        task_data["progress"] = 0
        task_data["tracks"] = []
        task_data["retry_count"] = retry_count
        if redis_client:
            try:
                async with redis_client.pipeline(transaction=True) as pipe:
                    await pipe.set(f"task:{task_id}", json.dumps(task_data))
                    await pipe.set(f"callback_received:{task_id}", "false")
                    await pipe.publish(f"channel:{task_id}", json.dumps(task_data))
                    await pipe.execute()
                logger.info(f"Retry initiated for task_id: {task_id}, data: {json.dumps(task_data, indent=2)}")
            except Exception as e:
                logger.error(f"Redis error during retry for task_id: {task_id}: {str(e)}")
                return {"msg": "error", "message": f"Redis error: {str(e)}"}
        elif tasks is not None:
            tasks[f"task:{task_id}"] = task_data
        return {"msg": "success", "message": "Task retried", "data": {"taskId": task_id}}
    else:
        logger.error(f"Retry failed for task_id: {task_id}, error: {result.get('message')}")
        return result

# Song generation endpoint
@app.post("/generate", dependencies=[Depends(RateLimiter(times=100, seconds=3600))])
async def generate_song_endpoint(request: SongRequest, user: dict = Depends(get_current_user)):
    logger.info(f"Authenticated user: {user.get('sub')}")
    logger.info(f"Received generate request: {json.dumps(request.dict(), indent=2)}")
    if request.instrumental is None:
        request.instrumental = False
    if request.model is None:
        request.model = "V4_5"
    result = await generate_song(
        prompt=request.prompt,
        style=request.style,
        title=request.title,
        custom_mode=request.custom_mode,
        instrumental=request.instrumental,
        model=request.model,
        instruments=request.instruments,
        language=request.language,
        negative_tags=request.negative_tags,
        duration=request.duration,
        lyrics=request.lyrics,
        mood=request.mood,
        callBackUrl=request.callBackUrl
    )
    return result

# Status endpoint
@app.get("/status/{task_id}")
async def get_status(task_id: str, force: bool = False, user: dict = Depends(get_current_user)):
    task_data = None
    if redis_client:
        try:
            task_data = await redis_client.get(f"task:{task_id}")
        except Exception as e:
            logger.error(f"Redis error in get_status: {str(e)}")
    elif tasks is not None:
        task_data = tasks.get(f"task:{task_id}")

    if not task_data:
        logger.error(f"Task not found: {task_id}")
        raise HTTPException(status_code=404, detail="Task not found")

    data = json.loads(task_data) if redis_client else task_data
    if data["tracks"] and all(t.get("has_audio") and t.get("audio_url") for t in data["tracks"]):
        data["status"] = "done"
        data["progress"] = 100
        if redis_client:
            try:
                async with redis_client.pipeline(transaction=True) as pipe:
                    await pipe.set(f"task:{task_id}", json.dumps(data))
                    await pipe.publish(f"channel:{task_id}", json.dumps(data))
                    await pipe.execute()
                logger.info(f"Forced status to 'done' for task_id: {task_id}")
            except Exception as e:
                logger.error(f"Redis error updating status: {str(e)}")
        elif tasks is not None:
            tasks[f"task:{task_id}"] = data

    if force:
        logger.info(f"Forced status refresh for task_id: {task_id}")
        asyncio.create_task(poll_task_status(task_id))
    logger.info(f"Returning status for task_id: {task_id}, status: {data['status']}, progress: {data.get('progress', 0)}%, tracks: {json.dumps(data['tracks'], indent=2)}")
    return {
        "status": data["status"],
        "tracks": data["tracks"],
        "progress": data.get("progress", 0),
        "original_prompt": data.get("original_prompt"),
        "original_lyrics": data.get("original_lyrics")
    }

# Callback endpoint
@app.post("/callback")
async def handle_callback(request: Request):
    raw = await request.body()
    logger.info(f"RAW CALLBACK BODY: {raw}")
    try:
        data = await request.json()
        logger.info(f"Parsed callback: {json.dumps(data, indent=2)}")
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {str(e)}, raw body: {raw.decode('utf-8', errors='ignore')}")
        raise HTTPException(status_code=400, detail=f"Invalid JSON in callback: {str(e)}")

    try:
        payload = FullCallbackPayload(**data)
        task_id = payload.data.task_id
        if redis_client:
            try:
                if await redis_client.exists(f"callback_lock:{task_id}"):
                    logger.warning(f"Duplicate callback for task_id: {task_id}")
                    return {"msg": "duplicate", "message": "Callback already processed"}
            except Exception as e:
                logger.error(f"Redis error checking callback lock: {str(e)}")
        elif tasks is not None and f"callback_lock:{task_id}" in tasks:
            logger.warning(f"Duplicate callback for task_id: {task_id}")
            return {"msg": "duplicate", "message": "Callback already processed"}

        callback_type = payload.data.callbackType.lower()
        logger.info(f"Step 1/3 - Processing callback type: {callback_type} for task_id: {task_id}")
        status = "pending"
        progress = 30 if callback_type == "text" else 60 if callback_type == "first" else 100
        if callback_type == "complete":
            status = "done"

        task_data = None
        original_prompt = None
        original_lyrics = None
        task_data_dict = {}
        if redis_client:
            try:
                task_data = await redis_client.get(f"task:{task_id}")
                if task_data:
                    task_data_dict = json.loads(task_data)
                    original_prompt = task_data_dict.get("original_prompt")
                    original_lyrics = task_data_dict.get("original_lyrics")
            except Exception as e:
                logger.error(f"Redis error fetching task data: {str(e)}")
        elif tasks is not None:
            task_data = tasks.get(f"task:{task_id}")
            if task_data:
                task_data_dict = task_data
                original_prompt = task_data_dict.get("original_prompt")
                original_lyrics = task_data_dict.get("original_lyrics")

        start_time = time.time()
        tracks = []
        for track in payload.data.data:
            logger.info(f"Step 2/3 - Track raw data: {track.dict()}")
            audio_url = track.source_stream_audio_url or track.source_audio_url or track.stream_audio_url or track.audio_url or ""
            lyrics = track.lyrics or track.prompt if not task_data_dict.get("instrumental") else ""
            tracks.append({
                "id": track.id,
                "title": track.title or "Untitled",
                "audio_url": audio_url,
                "image_url": track.image_url or track.source_image_url or "",
                "prompt": track.prompt or original_prompt or "",
                "has_audio": bool(audio_url),
                "lyrics": lyrics,
                "tags": track.tags or payload.data.data[0].tags if payload.data.data and payload.data.data[0].tags else ""
            })
        logger.info(f"Step 3/3 - Track data prepared (duration: {time.time() - start_time:.2f}s)")

        asyncio.create_task(validate_and_update_audio(task_id, tracks, original_prompt, original_lyrics))

        task_data = {
            "status": status,
            "tracks": tracks,
            "progress": progress,
            "original_prompt": original_prompt,
            "original_lyrics": original_lyrics,
            "created_at": int(time.time()),
            "style": task_data_dict.get("style", ""),
            "title": task_data_dict.get("title", ""),
            "custom_mode": task_data_dict.get("custom_mode", False),
            "instrumental": task_data_dict.get("instrumental", False),
            "model": task_data_dict.get("model", "V4_5"),
            "instruments": task_data_dict.get("instruments", None),
            "language": task_data_dict.get("language", None),
            "negative_tags": task_data_dict.get("negative_tags", None),
            "duration": task_data_dict.get("duration", None),
            "mood": task_data_dict.get("mood", None),
            "callBackUrl": task_data_dict.get("callBackUrl", DEFAULT_CALLBACK_URL)
        }
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if redis_client:
                    async with redis_client.pipeline(transaction=True) as pipe:
                        await pipe.set(f"task:{task_id}", json.dumps(task_data))
                        await pipe.set(f"callback_received:{task_id}", "true")
                        await pipe.set(f"callback_lock:{task_id}", "true", ex=10)
                        prompt_hash = hashlib.md5((original_prompt + (original_lyrics or "")).encode()).hexdigest() if original_prompt else ""
                        if prompt_hash and status == "done":
                            await pipe.set(f"prompt:{prompt_hash}", json.dumps({"msg": "success", "data": {"taskId": task_id}}))
                        await pipe.incr(f"downloads:{task_id}:count")
                        await pipe.hset(f"analytics:{task_id}", mapping={
                            "prompt": original_prompt or "",
                            "style": payload.data.data[0].tags.split(",")[0] if payload.data.data and payload.data.data[0].tags else "",
                            "timestamp": str(payload.data.data[0].createTime) if payload.data.data else "",
                            "original_prompt_length": len(original_prompt) if original_prompt else 0,
                            "original_lyrics_length": len(original_lyrics) if original_lyrics else 0
                        })
                        await pipe.publish(f"channel:{task_id}", json.dumps(task_data))
                        await pipe.execute()
                elif tasks is not None:
                    tasks[f"task:{task_id}"] = task_data
                    tasks[f"callback_received:{task_id}"] = "true"
                    tasks[f"callback_lock:{task_id}"] = "true"
                    prompt_hash = hashlib.md5((original_prompt + (original_lyrics or "")).encode()).hexdigest() if original_prompt else ""
                    if prompt_hash and status == "done":
                        tasks[f"prompt:{prompt_hash}"] = {"msg": "success", "data": {"taskId": task_id}}
                logger.info(f"Redis updated for task_id: {task_id}, data: {json.dumps(task_data, indent=2)}")
                if status == "done":
                    logger.info(f"Delaying unsubscription for task_id: {task_id} to ensure SSE delivery")
                    await asyncio.sleep(1)
                if redis_client:
                    try:
                        await redis_client.bgrewriteaof()
                    except Exception as e:
                        logger.error(f"Redis bgrewriteaof error: {str(e)}")
                break
            except Exception as e:
                logger.error(f"Redis write failed for task_id: {task_id}, attempt {attempt + 1}/{max_retries}: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)
                else:
                    raise HTTPException(status_code=503, detail=f"Redis write error: {str(e)}")

        logger.info(f"Callback processed for task_id: {task_id}, status: {status}, progress: {progress}%, tracks: {json.dumps(tracks, indent=2)}")
        return {"msg": "success", "message": "Callback processed"}
    except Exception as e:
        logger.error(f"Callback validation failed: {str(e)}")
        raise HTTPException(status_code=422, detail=f"Callback validation error: {str(e)}")

# SSE endpoint for real-time status updates
@app.get("/events/{task_id}")
async def status_events(task_id: str, user: dict = Depends(get_current_user)):
    async def event_generator():
        if not redis_client and tasks is None:
            logger.error("Redis and in-memory storage not available for SSE")
            yield f"event: error\ndata: {json.dumps({'message': 'Storage not available'})}\n\n"
            return

        try:
            pubsub = redis_client.pubsub() if redis_client else None
            if pubsub:
                try:
                    await pubsub.subscribe(f"channel:{task_id}")
                    logger.info(f"Subscribed to channel: channel:{task_id}")
                    yield f": connected\n\n"
                    await asyncio.sleep(0)
                except Exception as e:
                    logger.error(f"Redis error subscribing to channel: {str(e)}")

            task_data = None
            if redis_client:
                try:
                    task_data = await redis_client.get(f"task:{task_id}")
                except Exception as e:
                    logger.error(f"Redis error fetching task data: {str(e)}")
            elif tasks is not None:
                task_data = tasks.get(f"task:{task_id}")

            if task_data:
                parsed = json.loads(task_data) if redis_client else task_data
                logger.info(f"Initial task data for {task_id}: {json.dumps(parsed, indent=2)}")
                yield f"event: status\ndata: {json.dumps(parsed)}\n\n"
                await asyncio.sleep(0)

            for _ in range(600):  # Support up to 10 minutes
                if pubsub:
                    try:
                        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                        if msg and msg["type"] == "message":
                            logger.info(f"SSE message sent for {task_id}: {msg['data']}")
                            yield f"event: status\ndata: {msg['data']}\n\n"
                            await asyncio.sleep(0)
                            parsed = json.loads(msg['data'])
                            if parsed["status"] == "done" and parsed["tracks"] and all(t.get("has_audio") and t.get("audio_url") for t in parsed["tracks"]):
                                logger.info(f"Completed status sent for {task_id}, waiting 1s before unsubscribe")
                                await asyncio.sleep(1)
                                break
                            if parsed["status"] == "failed" and parsed["tracks"]:
                                logger.info(f"Failed status sent for {task_id}, closing SSE")
                                yield f"event: error\ndata: {json.dumps({'message': 'Task failed, tracks received but invalid'})}\n\n"
                                break
                    except Exception as e:
                        logger.error(f"Redis error in SSE: {str(e)}")

                task_data = None
                if redis_client:
                    try:
                        task_data = await redis_client.get(f"task:{task_id}")
                    except Exception as e:
                        logger.error(f"Redis error polling task data: {str(e)}")
                elif tasks is not None:
                    task_data = tasks.get(f"task:{task_id}")

                if task_data:
                    parsed = json.loads(task_data) if redis_client else task_data
                    logger.info(f"Polled task data for {task_id}: {json.dumps(parsed, indent=2)}")
                    yield f"event: status\ndata: {json.dumps(parsed)}\n\n"
                    await asyncio.sleep(0)
                    if parsed["status"] == "done" and parsed["tracks"] and all(t.get("has_audio") and t.get("audio_url") for t in parsed["tracks"]):
                        logger.info(f"Completed status polled for {task_id}, waiting 1s before unsubscribe")
                        await asyncio.sleep(1)
                        break
                    if parsed["status"] == "failed" and parsed["tracks"]:
                        logger.info(f"Failed status polled for {task_id}, closing SSE")
                        yield f"event: error\ndata: {json.dumps({'message': 'Task failed, tracks received but invalid'})}\n\n"
                        break

                if _ % 3 == 0:
                    logger.debug(f"Sending keep-alive for {task_id}")
                    yield ": keep-alive\n\n"
                    await asyncio.sleep(0)

                await asyncio.sleep(1)

            if pubsub:
                try:
                    await pubsub.unsubscribe(f"channel:{task_id}")
                    logger.info(f"Unsubscribed from channel: channel:{task_id}")
                except Exception as e:
                    logger.error(f"Redis error unsubscribing from channel: {str(e)}")
        except Exception as e:
            logger.exception(f"SSE error for task {task_id}: {e}")
            yield f"event: error\ndata: {json.dumps({'message': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
            "Content-Encoding": "identity"
        }
    )