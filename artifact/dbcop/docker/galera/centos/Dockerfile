FROM centos:7

RUN echo -e "[mariadb]\nname = MariaDB\nbaseurl = http://yum.mariadb.org/10.1/centos7-amd64\nenabled = 1\ngpgkey = https://yum.mariadb.org/RPM-GPG-KEY-MariaDB\ngpgcheck = 1"  > /etc/yum.repos.d/MariaDB.repo

RUN rpmkeys --import https://www.percona.com/downloads/RPM-GPG-KEY-percona && \
	yum install -y http://www.percona.com/downloads/percona-release/redhat/0.1-4/percona-release-0.1-4.noarch.rpm
RUN yum install -y which MariaDB-server MariaDB-client socat percona-xtrabackup && \
	yum clean all 

ADD galera.cnf /etc/my.cnf.d/galera.cnf
ADD entrypoint.sh /entrypoint.sh

ENTRYPOINT ["bash", "/entrypoint.sh"]
