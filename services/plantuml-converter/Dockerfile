FROM python:3.11-slim

# Install Java (required for PlantUML)
RUN apt-get update && apt-get install -y \
    default-jre \
    graphviz \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Download PlantUML jar
COPY plantuml-1.2024.6.jar ./plantuml.jar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY plantuml_converter.py .
COPY config.yaml .

# Run the Python script directly
CMD ["python", "plantuml_converter.py"]
