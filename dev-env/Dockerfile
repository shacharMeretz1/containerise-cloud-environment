FROM python:3 AS prepare-image
WORKDIR /usr/src/app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


FROM python:3 AS run-image
WORKDIR /usr/src/app
COPY --from=prepare-image /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN eval $(ssh-agent -s)
COPY id_rsa /root/.ssh
COPY id_rsa.pub /root/.ssh
RUN chmod 600 ~/.ssh/id_rsa
COPY brie_script.sh .
RUN chmod +x deploy_script.sh
ARG ENV_NAME
COPY ${ENV_NAME}.env .


