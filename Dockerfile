FROM quay.io/jupyter/base-notebook

# Install aqora-cli
RUN pip install aqora-cli

# Ensure the PATH is correct
ENV PATH="${PATH}:/home/jovyan/.local/bin"