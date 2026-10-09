# Google's pull-through mirror of Docker Hub: same image (same digest), but CI runners are not
# subject to Docker Hub's anonymous pull limit there (429 Too Many Requests).
FROM mirror.gcr.io/prefecthq/prefect:3.7.1-python3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
