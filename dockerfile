FROM python:3.10.9-slim

RUN addgroup --system app && adduser --system --group app

USER app

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY .env .

COPY flightradar.py .

ENTRYPOINT [ "python3", "flightradar.py" ]