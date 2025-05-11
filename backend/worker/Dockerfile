# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables for unbuffered Python output and PYTHONPATH
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /app

# Set the working directory in the container
WORKDIR /app

# Set WORKER_ID_PREFIX
ENV WORKER_ID_PREFIX="worker"

# Copy the entire backend directory content into /app
COPY . /app

# Install dependencies
# The requirements file will be at /app/worker/requirements.txt
RUN pip install --no-cache-dir -r /app/worker/requirements.txt

# Download NLTK data (if necessary, text_utils handles this lazily now)
# RUN python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords')"

# The imports like 'from backend.shared.text_utils' should work due to PYTHONPATH=/app
# and the file structure /app/backend/shared/text_utils.py.

# Run worker.py when the container launches
CMD ["python", "-u", "worker/worker.py"]