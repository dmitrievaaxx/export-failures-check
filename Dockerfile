FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
ENV PORT=8000
# Сервис работает не от root: пользователь nobody (65534), папка для данных — его
# (том app-data в compose.yaml создаётся с её владельцем).
RUN mkdir -p /data && chown 65534:65534 /data
USER 65534:65534
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
