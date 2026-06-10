# Backup with Globus Transfer

Walkthough on how to setup a Globus timed flow to periodically (and automatically) transfer log files from the Gateway API's host to a persistent storage (Globus Guest collection).

## Guest Collection for Gateway Host

### Create Globus Personal Connect Transfer Endpoint
```bash
sudo -u webportal /bin/bash
cd ~
wget https://downloads.globus.org/globus-connect-personal/linux/stable/globusconnectpersonal-latest.tgz
tar xzf globusconnectpersonal-latest.tgz
rm globusconnectpersonal-latest.tgz
cd globusconnectpersonal-3.2.8/
./globusconnectpersonal -setup
```

### Systemctl Service
```bash
cd ../
mv globusconnectpersonal-3.2.8/ ~/.globusconnectpersonal
```

Add the following to `~/.globusonline/lta/config-paths` (first 1 - shareable, second 0 - read-only access)
```bash
/var/log/inference-service/,1,0
/home/webportal/inference-gateway/pg_backup
```

With `sudo`, add the following in `/etc/systemd/system/globusconnectpersonal.service`
```bash
[Unit]
Description=Globus Connect Personal to transfer data to persistent storage
After=network.target

[Service]
User=webportal
Group=webportal
ExecStart=/home/webportal/.globusconnectpersonal/globusconnectpersonal -start

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable globusconnectpersonal
sudo systemctl start globusconnectpersonal
```

Visit the Mapped collection in the Gloubus web page, and create Guest Collections targetting the same paths as in `~/.globusonline/lta/config-paths`.

## Guest Collection for Storage

Create a Guest Collection within your HPC storage. Make sure the user owning the Globus Connect Personal endpoint is part of the unix group on the targetted HPC storage folder. Make sure the group has write permission on that folder.


## Setting a Timed Globus Flow

On the source collection, click on filter and select 
```
*.log.*
```


Initiate a transfer, and choose the folloing options:
* label this transfer: inference-logs-backup
* apply sync level L1
* preserve source file modifications times
* encrypt tranfers
* fail on quota error
* apply filter rules to the transfer
    * include files \*.log\*
    * include files \*.json