FROM python:3.7-slim

RUN useradd -ms /bin/bash frappe
USER frappe
ENV HOME /home/frappe
ENV PATH $PATH:$HOME/.local/bin

RUN mkdir /home/frappe/agent && \
  mkdir /home/frappe/repo && \
  chown -R frappe:frappe /home/frappe

COPY --chown=frappe:frappe requirements.txt /home/frappe/repo/
RUN pip install --user --requirement /home/frappe/repo/requirements.txt

COPY --chown=frappe:frappe . /home/frappe/repo/
RUN pip install --user --editable /home/frappe/repo

WORKDIR /home/frappe/agent
