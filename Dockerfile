FROM python:3.11-slim

WORKDIR /app

# Copy the project files
COPY . .

# Install the package and its dependencies
RUN pip install --no-cache-dir .

# The main entry point is main.py, not server.py
CMD ["python", "main.py"]