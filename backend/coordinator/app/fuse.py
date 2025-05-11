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

# Example for understanding and testing (not run by the application directly)
if __name__ == '__main__':
    # Setup basic logging for direct testing of this module if no handlers are configured
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("--- Testing fuse.py logic ---")
    
    _global_idx_test: Dict[str, Dict[str, int]] = {
        "apple": {"doc1.txt": 2, "doc2.txt": 1},
        "banana": {"doc1.txt": 1}
    }
    _test_lock = threading.Lock() # For testing thread safety

    logger.info(f"Initial global_index: {_global_idx_test}")

    # Test 1: New document, new and existing terms
    partial1_doc_id = "doc3.txt"
    partial1_data = {
        "apple": {partial1_doc_id: 3},    # Existing term, new doc
        "orange": {partial1_doc_id: 5}  # New term, new doc
    }
    logger.info(f"\nMerging Test 1 (doc: {partial1_doc_id}, data: {partial1_data})")
    merge_partial_index(_global_idx_test, partial1_data, partial1_doc_id, lock=_test_lock)
    logger.info(f"Global_index after Test 1: {_global_idx_test}")
    # Expected: apple: {d1:2, d2:1, d3:3}, banana: {d1:1}, orange: {d3:5}

    # Test 2: Existing document, updating frequency and adding new term for it
    partial2_doc_id = "doc1.txt"
    partial2_data = {
        "banana": {partial2_doc_id: 4}, # Update frequency for existing term in existing doc
        "grape": {partial2_doc_id: 2}   # New term for existing doc
    }
    logger.info(f"\nMerging Test 2 (doc: {partial2_doc_id}, data: {partial2_data})")
    merge_partial_index(_global_idx_test, partial2_data, partial2_doc_id, lock=_test_lock)
    logger.info(f"Global_index after Test 2: {_global_idx_test}")
    # Expected: apple: {d1:2, d2:1, d3:3}, banana: {d1:4}, orange: {d3:5}, grape: {d1:2}

    # Test 3: Malformed partial index - doc_id in map key does not match doc_id_processed
    partial3_doc_id = "doc4.txt"
    partial3_data_malformed_key = {
        "fig": {"wrong_doc.txt": 1} # Key should be "doc4.txt"
    }
    logger.info(f"\nMerging Test 3 (doc: {partial3_doc_id}, data: {partial3_data_malformed_key}) - Malformed key")
    merge_partial_index(_global_idx_test, partial3_data_malformed_key, partial3_doc_id, lock=_test_lock)
    logger.info(f"Global_index after Test 3: {_global_idx_test}") # Should be unchanged from Test 2 output

    # Test 4: Malformed partial index - non-integer frequency
    partial4_doc_id = "doc5.txt"
    partial4_data_malformed_value = {
        "kiwi": {partial4_doc_id: "not_a_number"} # type: ignore
    }
    logger.info(f"\nMerging Test 4 (doc: {partial4_doc_id}, data: {partial4_data_malformed_value}) - Malformed value")
    merge_partial_index(_global_idx_test, partial4_data_malformed_value, partial4_doc_id, lock=_test_lock)
    logger.info(f"Global_index after Test 4: {_global_idx_test}") # Should be unchanged

    # Test 5: Empty partial index
    partial5_doc_id = "doc6.txt"
    partial5_data_empty = {}
    logger.info(f"\nMerging Test 5 (doc: {partial5_doc_id}, data: {partial5_data_empty}) - Empty partial index")
    merge_partial_index(_global_idx_test, partial5_data_empty, partial5_doc_id, lock=_test_lock)
    logger.info(f"Global_index after Test 5: {_global_idx_test}") # Should be unchanged

    # Test 6: Term data is not a dict
    partial6_doc_id = "doc7.txt"
    partial6_data_wrong_type = {
        "lemon": "not_a_dict" # type: ignore
    }
    logger.info(f"\nMerging Test 6 (doc: {partial6_doc_id}, data: {partial6_data_wrong_type}) - Term data not a dict")
    merge_partial_index(_global_idx_test, partial6_data_wrong_type, partial6_doc_id, lock=_test_lock)
    logger.info(f"Global_index after Test 6: {_global_idx_test}") # Should be unchanged

    logger.info("\n--- fuse.py testing finished. ---") 