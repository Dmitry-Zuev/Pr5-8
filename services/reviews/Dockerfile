FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt /srv/requirements.txt
RUN pip install --no-cache-dir -r /srv/requirements.txt
COPY common /srv/common
COPY services /srv/services
COPY rabbitmq /srv/rabbitmq
CMD ["uvicorn", "services.reviews.main:app", "--host", "0.0.0.0", "--port", "8000"]
