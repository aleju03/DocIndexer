# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables for unbuffered Python output and PYTHONPATH
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /app

# Set the working directory in the container
WORKDIR /app

# Copy the entire backend directory content into /app
COPY . /app

# Install dependencies
# The requirements file will be at /app/coordinator/requirements.txt
RUN pip install --no-cache-dir -r /app/coordinator/requirements.txt

# Download NLTK data (if necessary, text_utils handles this lazily now)
# RUN python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords')"

# The imports like 'from backend.shared.text_utils' and 
# 'from backend.coordinator.app.models' should work due to PYTHONPATH=/app
# and the file structure like /app/backend/coordinator/app/models.py.

# Expose the port the app runs on
EXPOSE 8000

# Run the FastAPI application using Uvicorn
CMD ["uvicorn", "coordinator.app.main:app", "--host", "0.0.0.0", "--port", "8000"] 