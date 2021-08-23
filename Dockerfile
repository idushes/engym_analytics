FROM python:3.9
WORKDIR /app
RUN mkdir -p /dataset
COPY . .
RUN rm -r env
RUN pip install -r requirements.txt
CMD python3 main.py