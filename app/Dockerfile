FROM python:3.7-slim-buster

WORKDIR /app

# set env variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt .

EXPOSE 8000

RUN pip install -r requirements.txt

COPY . .

CMD ["uvicorn", "server_app:app", "--host", "0.0.0.0", "--port", "8000"]