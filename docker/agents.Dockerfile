# Agents service: DeepSeek-driven agents + local embeddings. Kept out of the Airflow image on purpose:
# torch/sentence-transformers would conflict with Airflow's constraints and add ~1 GB to every Airflow task.
FROM python:3.12-slim
RUN pip install --no-cache-dir torch==2.15.0 --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir sentence-transformers==6.3.0 "psycopg[binary]==3.3.5" pydantic==2.13.5 \
      pydantic-settings==2.15.0 requests==2.34.2 tenacity==9.1.4 jinja2==3.1.6 pyyaml==6.0.4 sqlglot==30.35.0 \
      pytest==9.2.0
# Bake the embedding model into the image: no download at runtime, works offline.
ENV HF_HOME=/models
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"
ENV HF_HUB_OFFLINE=1 PYTHONPATH=/opt/lakehouse/src:/opt/lakehouse
WORKDIR /opt/lakehouse
CMD ["python", "-u", "-m", "agents.service"]
