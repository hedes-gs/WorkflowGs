# Archive and manage your RAW Sony images

If you need to manage a lot of RAW images, this project may interest you. Thanks to some big data technologies, you are allowed to :
- extract EXIF and THUMB images from a RAW image
- persist them in a hbase database using an hdfs system file
- archive the RAW images in a hdfs cluster

Currently, it is validated on a cluster of 6 machines like https://www.fit-pc.com/web/products/ipc2 running the linux distribution Ubuntu, each one having an Intel I7, 16Go, 1 disk 512Gb SSD, and 1 2To disk.
As this is running on a hbase/hdfs/kafka cluster, you can quickly add a new machine to increase automatically the available disk space to store more images. This is a main advantage on a NAS.


the big data technologies used are :
- kafka, kafka stream
- hadoop, hbase
- apache storm

Ansible scripts are used to deploy the binaries and to update the versions of kafka, hbase, hadoop and storm in the cluster.

Today only Sony RAW images are supported.

See our wiki https://github.com/hedes-gs/WorkflowGs/wiki for the Architecture diagrams.

# Access yourimages through a react UI
Though an UI, you can then
- retrieve them by date
- create some album
- tag some images with a person
