
FROM python:3.12

WORKDIR /Ejercicio-SCV

COPY . .

RUN apt-get update -o Acquire::Retries=3
RUN apt-get install -y python3-pip
RUN pip3 install --upgrade pip
RUN apt-get update
RUN apt-get install -y unixodbc-dev
RUN apt-get update
RUN pip install -r requirements.txt

EXPOSE 5000

CMD ["python", "main.py"]