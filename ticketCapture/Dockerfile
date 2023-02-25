FROM python:3.7

COPY . /app/ticketCapture

WORKDIR /app/ticketCapture

RUN pip install -r requirements.txt

WORKDIR /app


#Run the python 

#CMD FLASK_APP=ticketCapture/capture.py flask run --host 0.0.0.0 -p 80
CMD python ticketCapture/capture.py run

