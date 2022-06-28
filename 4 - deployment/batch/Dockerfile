FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

# RUN pip install -U pip
RUN pip install pipenv

COPY [ "Pipfile", "Pipfile.lock", "./" ]

RUN pipenv install --system --deploy -v

COPY [ "*.py", "./" ]

CMD [ "python", "predict.py" ]