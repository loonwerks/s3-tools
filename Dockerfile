ARG BASE_IMG=ubuntu:18.04
FROM $BASE_IMG

RUN apt-get -q update \
  && apt-get -y -q install python3 python3-pip \
  && apt-get clean autoclean \
  && apt-get autoremove --purge --yes \
  && rm -rf /var/lib/apt/lists/*

RUN pip3 install boto3
RUN pip3 install requests
RUN pip3 install BeautifulSoup4
RUN pip3 install awscli

