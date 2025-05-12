from typing import Dict, Optional
import threading
import logging # Import logging

# Get a logger for this module
logger = logging.getLogger(__name__)

# The global_index structure is expected to be:
# { "term": { "doc_id1": frequency, "doc_id2": frequency, ... }, ... }

# A partial index from a worker (the content of 'partial_index' in PartialIndexData model)
# for a specific document (e.g., doc_id = "docX.txt") looks like:
# { "termA": { "docX.txt": countA }, "termB": { "docX.txt": countB }, ... }

def merge_partial_index(
    global_index: Dict[str, Dict[str, int]],
    partial_index_from_worker: Dict[str, Dict[str, int]],
    doc_id_processed: str, # The document ID this partial index is for.
    lock: Optional[threading.Lock] = None
):
    """
    Merges a partial index from a single document (processed by a worker)
    into the global inverted index.

    Args:
        global_index: The main in-memory global inverted index.
        partial_index_from_worker: The partial index from one worker for one document.
                                   It's the 'partial_index' field of the PartialIndexData model.
        doc_id_processed: The specific document ID that partial_index_from_worker pertains to.
                          This is crucial for ensuring data integrity.
        lock: A threading.Lock object to ensure thread-safe updates to the global_index.
              The caller is responsible for acquiring and releasing the lock if provided elsewhere,
              or this function can manage it if it's the sole entry point for modification.
              For clarity, this function will use the lock if provided.
    """
    acquired_lock_internally = False
    if lock:
        lock.acquire()
        acquired_lock_internally = True
    
    try:
        logger.debug(f"Merging partial index for doc: {doc_id_processed}. Data has {len(partial_index_from_worker)} terms.")

        for term, doc_freq_map in partial_index_from_worker.items():
            if not isinstance(doc_freq_map, dict):
                logger.warning(f"Term '{term}' in partial index for doc '{doc_id_processed}' has invalid data type: {type(doc_freq_map)}. Expected dict. Skipping term.")
                continue

            # Validate that the doc_freq_map from the worker is for the *correct* doc_id
            # and contains the expected frequency.
            if doc_id_processed not in doc_freq_map:
                # This indicates a mismatch or malformed data from the worker.
                # The worker's calculate_tf should produce {term: {doc_id_processed: count}}.
                logger.error(f"Term '{term}' for doc '{doc_id_processed}': its own doc_id not found as a key in its frequency map {doc_freq_map}. This is unexpected. Skipping term.")
                continue
            
            frequency = doc_freq_map[doc_id_processed]
            if not isinstance(frequency, int) or frequency < 0:
                logger.warning(f"Term '{term}', doc '{doc_id_processed}': frequency '{frequency}' is not a non-negative integer. Skipping.")
                continue
            
            # Now, update the global_index
            if term not in global_index:
                global_index[term] = {}
            
            # Store/update the frequency of the term for this specific document.
            # If the document was processed before (e.g., re-indexing an updated doc),
            # this overwrites the previous frequency for this term in this doc.
            global_index[term][doc_id_processed] = frequency
            logger.debug(f"Updated global_index for term '{term}', doc '{doc_id_processed}' with freq {frequency}")

    finally:
        if acquired_lock_internally and lock:
            lock.release()