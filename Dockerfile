FROM python:3.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY stem_events ./stem_events
RUN useradd --create-home app && mkdir -p /data && chown app /data
USER app
CMD ["python", "-m", "stem_events.consumer"]
