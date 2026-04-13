FROM python:3.12-slim-bookworm

WORKDIR /app
RUN pip install --no-cache-dir gunicorn
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py index.html .

ENV PORT=8080
EXPOSE 8080

# Long timeout: folder archives can take a long time.
CMD gunicorn --bind 0.0.0.0:${PORT} --workers 1 --threads 8 --timeout 0 server:app
