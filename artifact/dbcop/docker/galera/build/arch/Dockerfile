FROM base/archlinux
RUN pacman -Syu --noconfirm
RUN pacman -S base-devel --noconfirm
RUN pacman -S galera rsync lsof --noconfirm
RUN pacman -S iproute2 --noconfirm
RUN mysql_install_db --user=mysql --basedir=/usr --datadir=/var/lib/mysql
RUN sed -e 's|wsrep_provider=.*|wsrep_provider=/usr/lib64/libgalera_smm.so|g' /etc/mysql/wsrep.cnf >> /etc/mysql/my.cnf
COPY mysql_first_time.sql '/tmp/mysql-first-time.sql'
EXPOSE 3306
ENTRYPOINT ["mysqld_safe", "--init-file=/tmp/mysql-first-time.sql"]
