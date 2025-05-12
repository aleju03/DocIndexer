import json
import os
import sys
import redis
import threading
import time
from typing import Callable, Optional, Tuple, List
import logging

# Relative import for models, assuming this file is part of the 'app' package
# For linters or direct execution, sys.path might need adjustment if 'app' is not recognized.
# However, for FastAPI running from the coordinator directory, this should work.
try:
    from .models import DocumentTask, PartialIndexData
except ImportError:
    # Fallback for scenarios where relative import fails (e.g. direct script run for testing)
    # This assumes that models.py is in the same directory or Python path is configured.
    from models import DocumentTask, PartialIndexData

# Get a logger for this module
logger = logging.getLogger(__name__)

# --- Configuration ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
# TASK_QUEUE_NAME is no longer a single global queue name, but a prefix.
TASK_QUEUE_PREFIX = os.getenv('REDIS_TASK_QUEUE_PREFIX', 'doc_processing_tasks') 
RESULTS_CHANNEL_NAME = os.getenv('REDIS_RESULTS_CHANNEL', 'idx_partial_results')

# --- Redis Client Initialization ---
_publisher_redis_client = None

def get_publisher_redis_client():
    """Manages a global Redis client instance for publishing tasks."""
    global _publisher_redis_client
    if _publisher_redis_client is None or not _publisher_redis_client.ping():
        try:
            _publisher_redis_client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, db=0,
                decode_responses=False # Tasks are JSON strings, Pydantic handles (de)serialization
            )
            _publisher_redis_client.ping() # Verify connection
            logger.info(f"Task Queue: Publisher Redis client connected to {REDIS_HOST}:{REDIS_PORT}")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Task Queue: ERROR connecting publisher Redis client - {e}", exc_info=True)
            _publisher_redis_client = None # Reset on failure
            raise # Re-raise to indicate failure
    return _publisher_redis_client

# Note: Subscriber client is created per-thread in start_results_listener

def get_least_loaded_worker(redis_conn: redis.Redis) -> Optional[str]:
    """
    Selects a worker based primarily on its current task queue length,
    and secondarily by reported CPU + RAM load.
    """
    worker_status_keys = redis_conn.keys("worker_status:*")
    if not worker_status_keys:
        logger.warning("get_least_loaded_worker: No worker_status keys found in Redis.")
        return None

    candidate_workers: List[Tuple[str, int, float]] = [] # worker_id, queue_length, load_metric

    for key_bytes in worker_status_keys:
        key = key_bytes.decode('utf-8')
        worker_id_from_key = key.split(":", 1)[1]
        
        # Check TTL to ensure worker is somewhat recent/alive
        ttl = redis_conn.ttl(key)
        if ttl < 0 and ttl != -1: # -2 means key doesn't exist (shouldn't happen here), -1 means no expire
            logger.debug(f"get_least_loaded_worker: Worker {worker_id_from_key} status key has expired or no TTL ({ttl}). Skipping.")
            continue

        status_data_bytes = redis_conn.hgetall(key)
        status_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in status_data_bytes.items()}

        try:
            cpu_str = status_data.get('cpu')
            ram_str = status_data.get('ram')
            
            cpu = float(cpu_str) if cpu_str is not None else 100.0 # Default to high load if missing
            ram = float(ram_str) if ram_str is not None else 100.0 # Default to high load if missing
        except (ValueError, TypeError) as e:
            logger.warning(f"get_least_loaded_worker: Error parsing status for worker {worker_id_from_key}: {e}. Defaulting to high load.")
            cpu = 100.0
            ram = 100.0
        
        load_metric = cpu + ram

        worker_specific_queue_name = f"{TASK_QUEUE_PREFIX}:{worker_id_from_key}"
        queue_length = redis_conn.llen(worker_specific_queue_name)
        if queue_length is None: # Should not happen with llen, but good to be safe
            logger.warning(f"get_least_loaded_worker: Could not get queue length for {worker_specific_queue_name}. Assuming high load.")
            queue_length = float('inf') # Effectively remove from consideration if llen fails

        candidate_workers.append((worker_id_from_key, queue_length, load_metric))
        logger.debug(f"get_least_loaded_worker: Candidate {worker_id_from_key} - QLen: {queue_length}, CPU: {cpu:.2f}, RAM: {ram:.2f}, Load: {load_metric:.2f}")

    if not candidate_workers:
        logger.warning("get_least_loaded_worker: No valid candidate workers found after checking status and queue length.")
        return None

    # Sort: first by queue_length (ascending), then by load_metric (ascending)
    candidate_workers.sort(key=lambda x: (x[1], x[2]))

    selected_worker_id = candidate_workers[0][0]
    logger.info(f"get_least_loaded_worker: Selected worker {selected_worker_id} (QLen: {candidate_workers[0][1]}, Load: {candidate_workers[0][2]:.2f}) from {len(candidate_workers)} candidates.")
    return selected_worker_id

# --- Task Publishing ---
def push_task_to_queue(doc_task: DocumentTask) -> Optional[int]:
    """
    Pushes a document processing task to the Redis queue of the least loaded worker.
    The task (DocumentTask model) is serialized to JSON.
    Returns the length of the list after the push operation, or None on error.
    """
    try:
        r_client = get_publisher_redis_client()
        if not r_client: # Ensure client is available
             logger.error("Task Queue: Cannot push task, Redis publisher client is not available.")
             return None

        task_json = doc_task.model_dump_json()
        
        # Use the improved selection logic
        worker_id = get_least_loaded_worker(r_client)
        
        if not worker_id:
            logger.error("Task Queue: No available or suitable workers found to assign the task.")
            return None
            
        # Construct the queue name for the selected worker
        # TASK_QUEUE_PREFIX is used here, e.g., "doc_processing_tasks"
        # So queue_name becomes e.g. "doc_processing_tasks:worker-xyz-123"
        queue_name = f"{TASK_QUEUE_PREFIX}:{worker_id}"
        
        logger.debug(f"Pushing task for doc_id: {doc_task.doc_id} to queue '{queue_name}' (assigned to worker {worker_id})")
        return r_client.rpush(queue_name, task_json)
    except redis.exceptions.RedisError as e:
        logger.error(f"Task Queue: ERROR pushing task to Redis queue: {e}", exc_info=True)
        global _publisher_redis_client
        _publisher_redis_client = None 
        return None
    except Exception as e:
        logger.error(f"Task Queue: ERROR serializing or pushing task: {e}", exc_info=True)
        return None

# --- Results Subscription ---
def start_results_listener(
    message_handler_callback: Callable[[PartialIndexData], None],
    stop_event: threading.Event
) -> threading.Thread:
    """
    Starts a Redis Pub/Sub listener in a separate daemon thread.
    Messages from RESULTS_CHANNEL_NAME are parsed into PartialIndexData
    and passed to the message_handler_callback.

    Args:
        message_handler_callback: Function to call with the parsed PartialIndexData.
        stop_event: A threading.Event object to signal the listener thread to stop.
    
    Returns:
        The started listener thread object.
    """
    logger.info(f"Task Queue: Attempting to start results listener on channel '{RESULTS_CHANNEL_NAME}'")
    
    def listener_thread_func():
        thread_id = threading.get_ident()
        logger.info(f"Task Queue (Thread {thread_id}): Listener thread started for '{RESULTS_CHANNEL_NAME}'.")
        
        r_sub_client = None
        pubsub = None

        while not stop_event.is_set():
            try:
                if r_sub_client is None or pubsub is None:
                    logger.info(f"Task Queue (Thread {thread_id}): Attempting to connect/subscribe to Redis...")
                    r_sub_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
                    r_sub_client.ping() # Test connection
                    pubsub = r_sub_client.pubsub(ignore_subscribe_messages=True)
                    pubsub.subscribe(RESULTS_CHANNEL_NAME)
                    logger.info(f"Task Queue (Thread {thread_id}): Subscribed to '{RESULTS_CHANNEL_NAME}'. Waiting for messages...")

                # Listen for messages with a timeout to allow checking stop_event
                message = pubsub.get_message(timeout=1.0) # seconds
                if stop_event.is_set(): break

                if message and message['type'] == 'message':
                    logger.debug(f"Task Queue (Thread {thread_id}): Received raw message on '{message['channel']}'")
                    message_data_str = message['data']
                    try:
                        data_dict = json.loads(message_data_str)
                        partial_index_obj = PartialIndexData(**data_dict)
                        message_handler_callback(partial_index_obj) # Process valid message
                    except json.JSONDecodeError as e_json:
                        logger.error(f"Task Queue (Thread {thread_id}): ERROR decoding JSON from Pub/Sub: {e_json}. Data: {message_data_str[:200]}...", exc_info=True)
                    except Exception as e_parse: # Covers Pydantic validation, etc.
                        logger.error(f"Task Queue (Thread {thread_id}): ERROR processing Pub/Sub message: {e_parse}. Data: {message_data_str[:200]}...", exc_info=True)
            
            except redis.exceptions.ConnectionError as e_conn:
                logger.warning(f"Task Queue (Thread {thread_id}): Redis connection error in listener: {e_conn}. Retrying in 5s...")
                if pubsub: pubsub.close(); pubsub = None
                if r_sub_client: r_sub_client.close(); r_sub_client = None
                time.sleep(5)
            except Exception as e_thread:
                logger.error(f"Task Queue (Thread {thread_id}): UNEXPECTED error in listener thread: {e_thread}. Retrying in 5s...", exc_info=True)

                if pubsub: pubsub.close(); pubsub = None
                if r_sub_client: r_sub_client.close(); r_sub_client = None
                time.sleep(5)
            if stop_event.is_set(): break

        # Cleanup when stop_event is set or loop exits
        if pubsub:
            try: pubsub.unsubscribe(RESULTS_CHANNEL_NAME); pubsub.close() 
            except Exception as e_close: logger.error(f"Task Queue (Thread {thread_id}): Error during pubsub close: {e_close}", exc_info=True)
        if r_sub_client: 
            try: r_sub_client.close()
            except Exception as e_rc_close: logger.error(f"Task Queue (Thread {thread_id}): Error during Redis client close: {e_rc_close}", exc_info=True)
        logger.info(f"Task Queue (Thread {thread_id}): Listener thread for '{RESULTS_CHANNEL_NAME}' terminated.")

    listener = threading.Thread(target=listener_thread_func, daemon=True)
    listener.start()
    logger.info(f"Task Queue: Results listener thread ({listener.ident if listener.ident else 'N/A'}) dispatched for '{RESULTS_CHANNEL_NAME}'.")
    return listener