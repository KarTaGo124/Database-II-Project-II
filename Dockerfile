FROM python:3.13-slim

WORKDIR /app


RUN apt-get update && apt-get install -y --no-install-recommends \
    libspatialindex-dev \
    libsndfile1 \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt'); nltk.download('punkt_tab')"

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "gui/app.py", "--server.port=8501", "--server.address=0.0.0.0", "--browser.serverAddress=localhost"]
