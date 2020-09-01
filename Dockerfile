ARG BASE_IMG=ubuntu:18.04
FROM $BASE_IMG

ARG UID
ARG UNAME
ARG GID
ARG GROUP

RUN groupadd -fg "${GID}" "${GROUP}" \
  && groupmod -g "${GID}" "${GROUP}" \
  && useradd -u "${UID}" -g "${GID}" "${UNAME}" \
  && passwd -d "${UNAME}" \
  && mkdir "/home/${UNAME}" \
  && chown -R "${UNAME}":"${GROUP}" "/home/${UNAME}" \
  && chmod -R ug+rw "/home/${UNAME}"

VOLUME /home/${UNAME}

RUN apt-get -q update \
  && apt-get -y -q install curl python3 python3-pip sudo wget \
  && apt-get clean autoclean \
  && apt-get autoremove --purge --yes \
  && rm -rf /var/lib/apt/lists/*

RUN pip3 install mako
RUN pip3 install boto3
RUN pip3 install requests
RUN pip3 install BeautifulSoup4
RUN pip3 install awscli
