FROM python:3.7.13-bullseye

ARG USER=appuser

ARG GROUP=appuser

ENV HOME /home/$USER

RUN groupadd --system $GROUP && \
    useradd --system --gid $USER $GROUP

WORKDIR $HOME/app

COPY --chown=$USER:$GROUP dependencies.txt ./

RUN pip3 install --no-cache-dir --upgrade pip --user && \
    pip3 install --no-cache-dir -r dependencies.txt --user

USER $USER

COPY --chown=$USER:$GROUP app/* ./

ENTRYPOINT ["python3", "./main.py"]

