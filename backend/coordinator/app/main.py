"""
Main application file for the Coordinator service.

Handles:
- FastAPI application setup.
- Managing the global inverted index (in-memory, loading/saving to JSON).
- API endpoints for document uploading and searching.
- Listening to Redis Pub/Sub for partial index results from workers.
- Dispatching document processing tasks to workers via Redis queue.
"""
import json
import os
import sys 
import threading
from typing import Dict, List, Tuple, Set, Optional
from contextlib import asynccontextmanager
import logging
import redis
import gzip

from fastapi import FastAPI, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware

# --- Logging Setup ---
LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)
logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("coordinator")

# --- Module Imports ---
# Relies on PYTHONPATH (e.g., /app in Docker) for module availability
try:
    from coordinator.app.models import (
        DocumentTask, PartialIndexData, SearchQuery, SearchResponse, StatusResponse, GlobalIndexStore, WorkerStatus, AllWorkersStatusResponse
    )
    from coordinator.app import task_queue
    from coordinator.app import fuse
    from shared.text_utils import normalize_text
except ImportError as e:
    logger.critical(f"CRITICAL: Failed to import necessary modules: {e}.", exc_info=True)
    logger.critical("Ensure that module paths (e.g., 'coordinator.app.models', 'shared.text_utils') are correct relative to PYTHONPATH=/app.")
    logger.critical("Coordinator cannot start without these core modules.")
    sys.exit(1)

# --- Configuration ---
# Path for local documents to be indexed by /trigger-local-indexing/
# Defaults to /app/backend/uploads inside the container (mounted from ./uploads in docker-compose)
DEFAULT_LOCAL_UPLOADS_DIR_IN_CONTAINER = "/app/backend/uploads"
LOCAL_UPLOADS_PATH = os.getenv("LOCAL_UPLOADS_PATH", DEFAULT_LOCAL_UPLOADS_DIR_IN_CONTAINER)

# Path for storing the persistent index file (e.g., /data/index.json in container)
DEFAULT_INDEX_FILE_STORAGE = "/data/index.json.gz"
INDEX_FILE_STORAGE_PATH = os.getenv("INDEX_FILE_STORAGE_PATH", DEFAULT_INDEX_FILE_STORAGE)
# Old `INDEX_FILE_PATH` constant removed.

# --- Global State ---
global_inverted_index: Dict[str, Dict[str, int]] = {}
index_lock = threading.Lock()
dispatched_docs_pending_results: Set[str] = set()
COORDINATOR_PROCESSING_LANGUAGE = os.getenv('COORDINATOR_PROCESSING_LANGUAGE', 'english')

# --- Helper Functions for Index Persistence ---
def load_global_index_from_file(file_path: str = INDEX_FILE_STORAGE_PATH) -> Dict[str, Dict[str, int]]:
    with index_lock:
        try:
            with gzip.open(file_path, "rt", encoding="utf8") as f:
                data = json.load(f)
                loaded_index = data.get('index', {}) 
                logger.info(f"Successfully loaded global index from {file_path}. {len(loaded_index)} terms.")
                return loaded_index
        except FileNotFoundError:
            logger.info(f"Index file {file_path} not found. Starting with an empty index.")
            return {}
        except json.JSONDecodeError as e:
            logger.warning(f"Error decoding JSON from {file_path}. Starting with an empty index. Error: {e}", exc_info=True)
            return {}
        except gzip.BadGzipFile:
            logger.warning(f"File {file_path} is not a valid gzip file. Starting with an empty index.")
            return {}
        except Exception as e:
            logger.error(f"An unexpected error occurred while loading index from {file_path}: {e}. Starting with an empty index.", exc_info=True)
            return {}

def save_global_index_to_file(file_path: str = INDEX_FILE_STORAGE_PATH):
    with index_lock:
        try:
            # Ensure the directory exists (e.g., /data/)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            index_content_to_save = {"index": global_inverted_index} 
            with gzip.open(file_path, "wt", encoding="utf8") as f:
                json.dump(index_content_to_save, f, ensure_ascii=False)
            logger.info(f"Successfully saved global index to {file_path}. {len(global_inverted_index)} terms.")
        except Exception as e:
            logger.error(f"Error saving global index to {file_path}: {e}", exc_info=True)

# --- Pub/Sub Message Handler ---
def handle_partial_index_message(data: PartialIndexData):
    logger.info(f"Received partial index from worker {data.worker_id} for doc {data.doc_id}")
    fuse.merge_partial_index(
        global_index=global_inverted_index,
        partial_index_from_worker=data.partial_index,
        doc_id_processed=data.doc_id,
        lock=index_lock
    )
    logger.debug(f"Global index updated for doc {data.doc_id}. Current total terms: {len(global_inverted_index)}")
    with index_lock:
        if data.doc_id in dispatched_docs_pending_results:
            dispatched_docs_pending_results.remove(data.doc_id)
            logger.info(f"Doc {data.doc_id} processing complete. {len(dispatched_docs_pending_results)} docs still pending.")
        else:
            logger.warning(f"Received results for doc {data.doc_id} which was not in pending set.")

# --- FastAPI Lifecycle Events (Startup/Shutdown) ---
stop_event_redis_listener = threading.Event()
redis_listener_thread: Optional[threading.Thread] = None # Type hint for clarity

@asynccontextmanager
async def lifespan(app_fastapi: FastAPI):
    global global_inverted_index, redis_listener_thread
    logger.info(f"Coordinator starting up... Log level: {LOG_LEVEL_STR}")
    logger.info(f"Expecting local documents from: {LOCAL_UPLOADS_PATH}")
    logger.info(f"Index persistence path: {INDEX_FILE_STORAGE_PATH}")
    global_inverted_index = load_global_index_from_file()
    logger.info("Text_utils (NLTK) will self-initialize on first use if needed.")
    try:
        redis_listener_thread = task_queue.start_results_listener(
            message_handler_callback=handle_partial_index_message,
            stop_event=stop_event_redis_listener
        )
        logger.info("Redis Pub/Sub listener for worker results started.")
    except Exception as e:
        logger.critical(f"Failed to start Redis Pub/Sub listener: {e}", exc_info=True)
    yield
    logger.info("Coordinator shutting down...")
    if redis_listener_thread and redis_listener_thread.is_alive():
        logger.info("Stopping Redis Pub/Sub listener...")
        stop_event_redis_listener.set()
        redis_listener_thread.join(timeout=10)
        if redis_listener_thread.is_alive():
            logger.warning("Redis listener thread did not stop cleanly after 10 seconds.")
        else:
            logger.info("Redis Pub/Sub listener stopped.")
    save_global_index_to_file()
    logger.info("Coordinator shutdown complete.")

app = FastAPI(
    title="Distributed Document Indexer - Coordinator API",
    description="Manages document indexing tasks and provides search functionality.",
    version="0.1.0",
    lifespan=lifespan
)

# CORS Middleware Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API Endpoints ---

@app.post("/trigger-local-indexing/", response_model=StatusResponse, status_code=202)
async def trigger_local_indexing_endpoint(path: Optional[str] = Form(None, description="Optional path to scan for .txt files. Defaults to configured LOCAL_UPLOADS_PATH.")):
    scan_path = path if path else LOCAL_UPLOADS_PATH
    logger.info(f"Triggering local indexing from path: {scan_path}")
    if not os.path.isdir(scan_path):
        logger.error(f"Local uploads path not found or is not a directory: {scan_path}")
        raise HTTPException(status_code=404, detail=f"Local uploads directory not found: {scan_path}")
    tasks_dispatched_count = 0
    successful_files: List[str] = []
    failed_files_details: List[Tuple[str, str]] = []
    files_found_count = 0
    for filename in os.listdir(scan_path):
        if filename.endswith(".txt"):
            files_found_count += 1
            file_path = os.path.join(scan_path, filename)
            doc_id = filename
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content_str = f.read()
                if not content_str.strip():
                    logger.warning(f"Document {doc_id} is empty or contains only whitespace. Skipping.")
                    failed_files_details.append((doc_id, "Skipped: File is empty or whitespace only"))
                    continue
                doc_language = COORDINATOR_PROCESSING_LANGUAGE
                task = DocumentTask(doc_id=doc_id, content=content_str)
                with index_lock:
                    dispatched_docs_pending_results.add(doc_id)
                pushed_length = task_queue.push_task_to_queue(task)
                if pushed_length is not None:
                    tasks_dispatched_count += 1
                    successful_files.append(doc_id)
                    logger.info(f"Dispatched task for local document: {doc_id}, language: {doc_language}. Queue length: {pushed_length}")
                else:
                    logger.warning(f"Failed to dispatch task for local document: {doc_id}")
                    failed_files_details.append((doc_id, "Failed to push to Redis queue"))
                    with index_lock:
                        if doc_id in dispatched_docs_pending_results:
                            dispatched_docs_pending_results.remove(doc_id)
            except Exception as e:
                logger.error(f"Error processing local file {doc_id}: {e}", exc_info=True)
                failed_files_details.append((doc_id, f"Error reading or dispatching: {str(e)[:100]}"))
                with index_lock:
                    if doc_id in dispatched_docs_pending_results:
                         dispatched_docs_pending_results.remove(doc_id)
    if files_found_count == 0:
        return StatusResponse(message=f"No .txt files found in {scan_path}. Nothing to index.", details={"successful_dispatches": [], "failed_files": [], "docs_currently_pending": len(dispatched_docs_pending_results)})
    return StatusResponse(message=f"Found {files_found_count} .txt files. Dispatched {tasks_dispatched_count} for indexing. {len(failed_files_details)} file(s) failed processing locally.", details={"successful_dispatches": successful_files, "failed_files": failed_files_details, "docs_currently_pending": len(dispatched_docs_pending_results)})

@app.post("/search/", response_model=SearchResponse)
async def search_endpoint(query: SearchQuery):
    if not query.term.strip(): raise HTTPException(status_code=400, detail="Search term cannot be empty.")
    stemmed_terms = normalize_text(query.term, language=COORDINATOR_PROCESSING_LANGUAGE)
    if not stemmed_terms: return SearchResponse(docs=[])
    search_stem = stemmed_terms[0]
    logger.info(f"Searching for original term: '{query.term}', processed stem: '{search_stem}', language: {COORDINATOR_PROCESSING_LANGUAGE}")
    results: List[Tuple[str, int]] = []
    with index_lock:
        if search_stem in global_inverted_index:
            doc_freq_map = global_inverted_index[search_stem]
            sorted_docs = sorted(doc_freq_map.items(), key=lambda item: item[1], reverse=True)
            results = [(doc_id, freq) for doc_id, freq in sorted_docs]
        else:
            logger.info(f"Stem '{search_stem}' not found in global index.")
    return SearchResponse(docs=results)

@app.get("/index-status/", response_model=StatusResponse)
async def index_status_endpoint():
    with index_lock:
        num_terms = len(global_inverted_index)
        num_pending_docs = len(dispatched_docs_pending_results)
    return StatusResponse(message="Current index status.", details={"total_terms_in_index": num_terms, "documents_pending_results": num_pending_docs})

@app.post("/index/save/", response_model=StatusResponse)
async def save_index_endpoint():
    try:
        save_global_index_to_file()
        logger.info(f"Manual save triggered. Index saved to {INDEX_FILE_STORAGE_PATH}")
        return StatusResponse(message=f"Global index saved to {INDEX_FILE_STORAGE_PATH}")
    except Exception as e:
        logger.error(f"Failed to manually save index to {INDEX_FILE_STORAGE_PATH}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save index: {e}")

@app.post("/index/load/", response_model=StatusResponse)
async def load_index_endpoint():
    global global_inverted_index
    logger.info(f"Manual load triggered. Attempting to load index from {INDEX_FILE_STORAGE_PATH}")
    loaded_index = load_global_index_from_file()
    with index_lock:
        global_inverted_index = loaded_index
        dispatched_docs_pending_results.clear()
    logger.info(f"Global index reloaded from {INDEX_FILE_STORAGE_PATH}. {len(global_inverted_index)} terms loaded.")
    return StatusResponse(message=f"Global index reloaded from {INDEX_FILE_STORAGE_PATH}. {len(global_inverted_index)} terms loaded.")

@app.get("/healthz", status_code=200)
async def health_check_endpoint():
    """Simple health check endpoint. Returns 200 OK if the app is running."""
    return {"status": "healthy", "message": "Coordinator is running"}

@app.get("/workers/status/", response_model=AllWorkersStatusResponse)
async def get_workers_status_endpoint():
    """Retrieves the current status (CPU, RAM, TTL, queue length) of all registered workers."""
    worker_statuses: List[WorkerStatus] = []
    try:
        r_client = task_queue.get_publisher_redis_client() # Re-use existing client logic
        if not r_client:
            logger.error("Cannot get worker statuses: Redis client unavailable.")
            raise HTTPException(status_code=503, detail="Service temporarily unavailable, cannot connect to Redis.")

        worker_status_keys = r_client.keys("worker_status:*")
        
        for key_bytes in worker_status_keys:
            key = key_bytes.decode('utf-8')
            worker_id_from_key = key.split(":", 1)[1] # e.g., worker_status:worker-hostname-pid -> worker-hostname-pid
            
            status_data_bytes = r_client.hgetall(key)
            status_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in status_data_bytes.items()}
            
            cpu = status_data.get('cpu')
            ram = status_data.get('ram')
            ttl = r_client.ttl(key)
            
            # Get worker-specific queue length
            worker_task_queue_name = f"doc_processing_tasks:{worker_id_from_key}"
            q_len = r_client.llen(worker_task_queue_name)

            worker_statuses.append(
                WorkerStatus(
                    worker_id=worker_id_from_key,
                    cpu_percent=float(cpu) if cpu is not None else None,
                    ram_percent=float(ram) if ram is not None else None,
                    status_ttl_seconds=ttl if ttl is not None and ttl >= 0 else None, # ttl can be -1 (no expire) or -2 (no key)
                    queue_length=q_len if q_len is not None else None
                )
            )
        # Sort workers by ID for consistent output
        worker_statuses.sort(key=lambda ws: ws.worker_id)
        return AllWorkersStatusResponse(workers=worker_statuses)

    except redis.exceptions.RedisError as e_redis:
        logger.error(f"Redis error while fetching worker statuses: {e_redis}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error communicating with Redis: {str(e_redis)}")
    except Exception as e_general:
        logger.error(f"Unexpected error while fetching worker statuses: {e_general}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e_general)}")

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Coordinator API server with Uvicorn for direct execution...")
    # LOG_LEVEL_STR is already defined at the top
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level=LOG_LEVEL_STR.lower()) 