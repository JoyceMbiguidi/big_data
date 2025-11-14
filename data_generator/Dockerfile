FROM python:3.11

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY data_simulation.py .

CMD ["python", "data_simulation.py"]
