# Both stream sources: Binance trades (producer.py) and the delayed yfinance equity feed (equity_producer.py).
FROM python:3.12-slim
RUN pip install --no-cache-dir websockets==16.1.1 confluent-kafka==2.15.1 yfinance==1.8.0
COPY streaming/producer.py streaming/equity_producer.py /app/
CMD ["python", "-u", "/app/producer.py"]
