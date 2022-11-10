# Builder stage
FROM python:3.8.14-slim as builder

WORKDIR /oee

ENV PYTHONDONTWRITEBYTECODE 1

ENV PYTHONUNBUFFERED 1

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev python-dev

RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .

RUN pip install -r requirements.txt

# Final stage
FROM python:3.8.14-slim

RUN apt-get update && \
    apt-get install libpq5 -y

ARG USER=oee

ARG GROUP=oee

RUN groupadd --system $GROUP && \
    useradd --system --gid $USER $GROUP

COPY --from=builder --chown=$USER:$GROUP /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

ENV HOME /home/$USER

WORKDIR $HOME/oee

RUN chown -R $USER:$GROUP $HOME/oee

USER $USER

COPY --chown=$USER:$GROUP json/ ./json/

COPY --chown=$USER:$GROUP src/ ./src/

WORKDIR $HOME/oee/src

ENTRYPOINT ["python3", "./main.py"]

