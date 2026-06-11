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

## Guest Collection for Storage

Create a Guest Collection within your HPC storage. Make sure the user owning the Globus Connect Personal endpoint is part of the unix group on the targetted HPC storage folder. Make sure the group has write permission on that folder. 

Create an empty folder with the same name as the one you want to backup. For example, if you want to backup the `/var/log/inference-service/` folder, create a `inference-service` folder on the HPC storage.

## Setting a Timed Globus Flow

Go to [https://app.globus.org/file-manager](https://app.globus.org/file-manager) and select the two-panel option, with the source collection on the left and the destination Guest collection on the right.

On the source collection on the Globus webapp, select the **folder** that you want to transfer. It needs to be a folder and not specific files, otherwise the Timed Transfer will not catch new files. For example, if you want to backup `/var/log/inference-service/`, make sure you navigate to `var/log` and check the box for `inference-service/`. For `/home/webportal/inference-gateway/pg_backup`, navigate to `/home/webportal/inference-gateway/` and check the box for `pg_backup`

On the destination Guest collection, simply navigate to the base of the collection without selecting any folder. This will guarantee that the `inference-service/` and `pg_backup/` folders are transfered and synced properly.

Initiate a transfer, and choose the folloing options:
* label this transfer
    * inference-logs-backup (if logs)
    * inference-database-backup (if database)
* apply sync level L2 (only transfer files that are new or modified)
* preserve source file modifications times
* encrypt tranfers
* fail on quota error
* Timer --> select start time and frequency

Monitor your Timers at [https://app.globus.org/timers](https://app.globus.org/timers)