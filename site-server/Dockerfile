FROM python:3.11

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app/__init__.py /code/app/__init__.py
COPY ./app/main.py /code/app/main.py
COPY ./app/quarto_sample/_site /code/app/quarto_sample/_site

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]