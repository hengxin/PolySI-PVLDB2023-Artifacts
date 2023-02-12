FROM archlinux/base
RUN pacman -Sy --noconfirm
RUN pacman -S galera --noconfirm
RUN pacman -S xtrabackup --noconfirm
RUN pacman -S grep awk iproute2 lsof rsync --noconfirm
ADD galera.cnf /etc/mysql/my.cnf.d/galera.cnf
ADD entrypoint.sh /entrypoint.sh
ENTRYPOINT ["bash", "/entrypoint.sh"]
