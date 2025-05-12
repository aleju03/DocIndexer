import json
import os
import sys
import time
import redis
import logging # Import logging
import psutil
import threading

# --- Logging Setup ---
# Configure logger for the worker module
LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)
logging.basicConfig(level=LOG_LEVEL,
                    format=f'%(asctime)s - %(levelname)s - [{os.getenv("HOSTNAME", "worker")}-{os.getpid()}] - %(message)s')
logger = logging.getLogger(__name__)

# --- Path setup for imports ---
_worker_dir = os.path.dirname(os.path.abspath(__file__))
_backend_dir = os.path.dirname(_worker_dir)
_project_root_parent = os.path.dirname(_backend_dir)
if _project_root_parent not in sys.path:
    sys.path.insert(0, _project_root_parent)

try:
    # Updated to import normalize_text and specify English as the default language
    from shared.text_utils import normalize_text 
except ImportError as e:
    logger.critical(f"Failed to import normalize_text: {e}. Ensure 'shared' package is in PYTHONPATH or run as module.", exc_info=True)
    # Fallback for critical failure if not runnable
    if "normalize_text" not in globals():
        def normalize_text(text:str, language:str="english") -> list[str]: 
            logger.critical("CRITICAL: normalize_text STUB is active. Text processing will fail.")
            return text.split()

# --- Configuration ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
TASK_QUEUE_NAME = os.getenv('REDIS_TASK_QUEUE', 'doc_processing_tasks')
RESULTS_CHANNEL_NAME = os.getenv('REDIS_RESULTS_CHANNEL', 'idx_partial_results')
PROCESSING_LANGUAGE = os.getenv('PROCESSING_LANGUAGE', 'english') # Configurable language

# Generate a unique worker ID for logging and tracking
hostname = os.getenv('HOSTNAME', 'local_host') # HOSTNAME is common in containers
pid = os.getpid()
WORKER_ID = f"worker-{hostname}-{pid}"

# Worker ID is now incorporated into the log format directly
WORKER_ID_LOG_PREFIX = f"[{os.getenv('HOSTNAME', 'local_host')}-{os.getpid()}]" # Kept for direct use if any print remains

# --- Redis Connection ---
redis_client = None

# Get a psutil.Process instance for the current process

_current_process = psutil.Process(os.getpid())
_num_cores = psutil.cpu_count() # Get number of logical CPU cores

def get_redis_client():
    """Establishes and returns a Redis client connection."""
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
            redis_client.ping()
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        except redis.exceptions.ConnectionError as e:
            logger.critical(f"Error connecting to Redis at {REDIS_HOST}:{REDIS_PORT} - {e}", exc_info=True)
            redis_client = None 
            raise  
    return redis_client

def calculate_tf(tokens: list[str], doc_id: str) -> dict:
    """
    Calculates Term Frequency (TF) for a list of tokens from a document.
    Output format: { "term1": {doc_id: count}, "term2": {doc_id: count}, ... }
    """
    tf_map = {}
    if not tokens: # Handle cases with no tokens after preprocessing
        return tf_map
    for token in tokens:
        if token not in tf_map:
            # Initialize with the structure expected by the coordinator for this term
            tf_map[token] = {doc_id: 0}
        # Increment count for the current document
        tf_map[token][doc_id] += 1
    return tf_map

def process_document_task(task_data_json: str):
    """
    Processes a single document task received from Redis.
    The task_data_json is expected to be a JSON string with 'doc_id' and 'content'.
    """
    try:
        r_client = get_redis_client()
        if not r_client:
            logger.error(f"No Redis client available, cannot process task. Task data: {task_data_json[:100]}...")
            return
    except redis.exceptions.ConnectionError:
        logger.error(f"Redis connection failed before processing task. Task data: {task_data_json[:100]}...")
        return # Cannot proceed without Redis

    task_data = None 
    doc_id_for_logging = "unknown_doc"
    try:
        task_data = json.loads(task_data_json)
        doc_id = task_data['doc_id']
        doc_id_for_logging = doc_id # Update for logging once known
        content = task_data['content']
        # Allow task to specify language, otherwise use worker's default
        language_to_use = task_data.get('language', PROCESSING_LANGUAGE)
        logger.info(f"Received task for doc_id: {doc_id}. Lang: {language_to_use}. Content len: {len(content)}")

        # 1. Preprocess text using normalize_text with the specified language
        processed_tokens = normalize_text(content, language=language_to_use)
        if not processed_tokens:
            logger.info(f"Doc ID {doc_id}: No tokens after normalization. Skipping TF calculation.")
            
            return

        logger.debug(f"Doc ID {doc_id}: Original words (approx) {len(content.split())}, Processed tokens {len(processed_tokens)}")

        # 2. Calculate Term Frequency (TF)
        partial_index = calculate_tf(processed_tokens, doc_id)
        logger.debug(f"Doc ID {doc_id}: Partial index generated with {len(partial_index)} terms.")

        # 3. Send partial index to coordinator via Redis Pub/Sub
        result_payload = {
            'worker_id': f'{os.getenv("HOSTNAME", "worker")}-{os.getpid()}', # Dynamic worker ID in payload
            'doc_id': doc_id,
            'partial_index': partial_index,
            'language': language_to_use # Include language in result for coordinator if needed
        }
        
        num_published = r_client.publish(RESULTS_CHANNEL_NAME, json.dumps(result_payload))
        if num_published > 0:
            logger.info(f"Doc ID {doc_id}: Published partial index to '{RESULTS_CHANNEL_NAME}' (to {num_published} subscribers).")
        else:
            logger.warning(f"Doc ID {doc_id}: Published partial index to '{RESULTS_CHANNEL_NAME}', but NO subscribers detected.")

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON task data: {task_data_json[:200]}...", exc_info=True)
    except KeyError:
        logger.error(f"Task data missing 'doc_id' or 'content'. Data: {str(task_data)[:200]}", exc_info=True)
    except redis.exceptions.RedisError as e:
        logger.error(f"Redis communication error processing task for doc_id {doc_id_for_logging}: {e}", exc_info=True)
        
    except Exception as e:
        logger.error(f"Unexpected error processing task for doc_id '{doc_id_for_logging}': {e}", exc_info=True)

def report_status_periodically(redis_conn, worker_id, interval=2):
    prev_cpu = -1.0  # Initialize to ensure first report always happens
    prev_ram = -1.0
    status_key = f"worker_status:{worker_id}"
    ttl_seconds = interval * 3 # e.g., if interval is 2s, TTL is 6s

    _current_process.cpu_percent() 
    time.sleep(0.1) # Small delay before starting the loop for more accurate first % reading

    while True:
        # Get CPU and RAM for the current process
        raw_cpu_percent = _current_process.cpu_percent()
        
        current_ram = _current_process.memory_percent()

        # Report only if there's a significant change or it's the first report
        # Update condition to use raw_cpu_percent instead of normalized_cpu for comparison
        if abs(raw_cpu_percent - prev_cpu) > 0.01 or abs(current_ram - prev_ram) > 0.01 or prev_cpu == -1.0:
            status_data = {
                "cpu": raw_cpu_percent, # Send raw CPU (relative to one core)
                "ram": current_ram,
            }
            try:
                redis_conn.hset(status_key, mapping=status_data)
                # Log with more precision for debugging
                logger.debug(f"Reported status for {worker_id}: CPU {raw_cpu_percent:.2f}% (raw, per-core), RAM {current_ram:.2f}%")
                prev_cpu = raw_cpu_percent # Store raw_cpu_percent for comparison
                prev_ram = current_ram
            except redis.exceptions.RedisError as e:
                logger.warning(f"Could not report status for {worker_id} to Redis: {e}")
        else:
            logger.debug(f"Status for {worker_id} largely unchanged (CPU {raw_cpu_percent:.2f}% raw, RAM {current_ram:.2f}%), skipping HMSET.")

        # Always update TTL to keep the key alive as long as the worker is running
        try:
            redis_conn.expire(status_key, ttl_seconds)
        except redis.exceptions.RedisError as e:
            logger.warning(f"Could not update TTL for {status_key} in Redis: {e}")

        time.sleep(interval)

def main_loop():
    """
    Main loop for the worker. Fetches tasks from the Redis queue and processes them.
    """
    logger.info(f"Worker starting. Default processing language: {PROCESSING_LANGUAGE.upper()}.")
    logger.info(f"Waiting for tasks on Redis queue 'doc_processing_tasks:{WORKER_ID}'. Log level: {LOG_LEVEL_STR}")
    logger.info(f"Number of logical CPU cores detected: {_num_cores if _num_cores else 'Unknown'}")
    
    # NLTK resources (stopwords) are downloaded by text_utils.py on its first import/use.
    # No explicit pre-initialization call needed here anymore.
    # If text_utils.py fails to init NLTK on first use, it will raise an error there.

    r_client = None
    worker_queue = f"doc_processing_tasks:{WORKER_ID}"
    status_thread = None
    while True:
        try:
            if r_client is None:
                r_client = get_redis_client()
                if status_thread is None and r_client: # Ensure r_client is valid
                    status_thread = threading.Thread(target=report_status_periodically, args=(r_client, WORKER_ID), daemon=True)
                    status_thread.start()
                    logger.info(f"Status reporting thread started for {WORKER_ID}.")

            if not r_client: # If still no client after attempt, wait and retry
                logger.warning("No Redis client for task fetching, retrying connection soon...")
                time.sleep(5)
                continue

            task_tuple = r_client.blpop(worker_queue, timeout=5)
            if task_tuple:
                _queue_name, task_data_json = task_tuple
                process_document_task(task_data_json)
            else:
                logger.debug("No task received in the last 5s, still waiting...")
                pass

        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection error in main loop: {e}. Attempting to reconnect in 5 seconds...", exc_info=True)
            if status_thread and status_thread.is_alive():
                 logger.info("Attempting to gracefully stop status thread due to main loop Redis error...")
                 # No direct stop for thread, rely on daemon and program exit or internal error handling in thread
            status_thread = None # Reset status thread so it can be restarted if redis reconnects
            redis_client = None 
            r_client = None     
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutdown signal (KeyboardInterrupt) received. Exiting gracefully.")
            break
        except Exception as e:
            # Catch-all for other unexpected errors in the main loop
            logger.critical(f"An unexpected error occurred in main_loop: {e}", exc_info=True)
            logger.info("Waiting for 5 seconds before retrying...")
            time.sleep(5)

if __name__ == "__main__":
    # The main_loop will be called. If text_utils is used for the first time
    # during a task, NLTK downloads will be attempted by text_utils itself.
    main_loop() 