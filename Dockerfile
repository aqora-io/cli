FROM python:3.11
RUN pip3 install \
    'jupyterhub==5.*' \
    'notebook==7.*' \
    "aqora-cli"

# create a user, since we don't want to run as root
RUN useradd -m jovyan
ENV HOME=/home/jovyan
WORKDIR $HOME
USER jovyan

CMD ["jupyterhub-singleuser"]