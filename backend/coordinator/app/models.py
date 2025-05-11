from typing import Dict, List, Tuple, Union, Optional
from pydantic import BaseModel, Field

class DocumentTask(BaseModel):
    """
    Represents a task to process a single document.
    This is what the coordinator will push to the Redis task queue.
    """
    doc_id: str = Field(..., description="Unique identifier for the document, e.g., filename.")
    content: str = Field(..., description="The raw text content of the document.")

class PartialIndexData(BaseModel):
    """
    Represents the partial index data sent by a worker for one document.
    Structure: { "term1": {"doc_id": count}, "term2": {"doc_id": count}, ... }
    This is received from Redis Pub/Sub, originally JSON string.
    """
    worker_id: str = Field(..., description="Identifier of the worker that processed the document.")
    doc_id: str = Field(..., description="Identifier of the document processed.")
    # The partial_index from a worker is specific to *its* doc_id.
    # Example from worker: { "perr": { "doc91.txt": 1 }, "corr": { "doc91.txt": 1 } }
    # So, each term maps to a dict that should only contain its own doc_id and its frequency in that doc.
    partial_index: Dict[str, Dict[str, int]] = Field(
        ...,
        description="The partial inverted index for the document processed by the worker. "
                    "Format: {'term': {'doc_id_processed_by_worker': frequency}}",
        example={"perr": {"doc91.txt": 1}, "corr": {"doc91.txt": 1}}
    )

class SearchQuery(BaseModel):
    """
    Represents a search query from the user, received by the API.
    """
    term: str = Field(..., description="The search term (keyword) to look for.")

# The backend.md shows: "docs": [ ["doc3.txt", 2], ["doc17.txt", 1] ]
# This is a list of lists/tuples, where each inner list/tuple is [doc_id, score_or_frequency].
# Pydantic can model this with List[Tuple[str, int]].

class SearchResponse(BaseModel):
    """
    Represents the search results returned to the user via API.
    Matches the JSON structure provided in backend.md.
    """
    docs: List[Tuple[str, int]] = Field(
        ...,
        description="List of [document_id, relevance_score_or_frequency] tuples/lists.",
        example=[("doc3.txt", 2), ("doc17.txt", 1), ("doc22.txt", 1)]
    )

class GlobalIndexStore(BaseModel):
    """
    Represents the structure of the global inverted index for storage (e.g., JSON file).
    The global index maps each term to a dictionary of document IDs and their frequencies.
    Format: { "term1": { "docA.txt": freqA1, "docB.txt": freqB1, ... },
              "term2": { "docC.txt": freqC2, ... }, ... }
    """
    index: Dict[str, Dict[str, int]] = Field(
        default_factory=dict,
        description="The global inverted index. Format: {'term': {'doc_id': frequency, ...}}",
        example={"report": {"doc3.txt": 2, "doc17.txt": 1}, "system": {"doc3.txt": 5}}
    )
    # We can add metadata if needed, e.g., last_updated_timestamp
    # total_docs_indexed: int = 0

# Example of a status message or simple response
class StatusResponse(BaseModel):
    message: str
    details: Dict = Field(default_factory=dict)

class WorkerStatus(BaseModel):
    worker_id: str = Field(..., description="Unique identifier of the worker.")
    cpu_percent: Optional[float] = Field(None, description="CPU utilization percentage of the worker.")
    ram_percent: Optional[float] = Field(None, description="RAM utilization percentage of the worker.")
    status_ttl_seconds: Optional[int] = Field(None, description="Time To Live (seconds) for the worker status key in Redis. Indicates freshness.")
    queue_length: Optional[int] = Field(None, description="Number of tasks pending in the worker's specific queue.")

class AllWorkersStatusResponse(BaseModel):
    workers: List[WorkerStatus] = Field(default_factory=list, description="List of current worker statuses.") 