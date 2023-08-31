FROM python:3.7
ENV AIRFLOW_HOME="/airflow"
COPY . .
RUN pip install -r requirements.txt
RUN chmod 777 start.sh
RUN airflow db init
RUN airflow users create  -e avnish@ineuron.ai -f Avnish -l Yadav -p admin -r Admin  -u admin
RUN apt update -y && apt install awscli -y
CMD ["./start.sh"]