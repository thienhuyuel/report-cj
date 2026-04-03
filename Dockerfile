# Nếu build từ thư mục cha: dùng Dockerfile ở gốc repo (../Dockerfile).
# Từ đúng thư mục web/:  docker build -t cj-report .
# Python 3.11 + libpff (PST) for libpff-python / pypff
FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpff-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY report_pipeline.py streamlit_app.py .
COPY .streamlit/ .streamlit/

ENV STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
