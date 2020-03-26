# vantiq-sftp-connector
sftp-connector for VANTIQ


## Usage
1. register SFTP Source type 
2. create a source in VANTIQ
3. start the connector

## register
Create a config file named *sftpSource.json*:
```
{
   "name" : "SFTP_Source",
   "baseType" : "EXTENSION",
   "verticle" : "service:extensionSource",
   "config" : {}
}
```

And run:
```
vantiq -s <profileName> load sourceimpls sftpSource.json
```

## Create source
In VANTIQ, you should see a new Source type named *SFTPProtoSource*, create a new source with this type, and config:
```
{
    "sftp_server_host": "localhost",
    "sftp_server_port": "22",
    "sftp_user": "username",
    "sftp_password": "thePassword",
    "remote_dir": "data/sub_data"
    "file_filter": "test_name"
}
```
`file_filter` is optional, if no provided, a default filter is *.DT* file extension.

## Package and Start connector
At first, package the connector with:
```
# package
mvn package -Dmaven.test.skip=true 

# and run
java -jar target/sftp-connector-1.0-SNAPSHOT-spring-boot.jar
```
You need a config file in current dir, the `config.json` file is:
```
{
    "vantiqUrl": "https://dev.vantiq.com.cn",
    "token": "<the_token>",
    "sourceName": "sftp_connector"
}
```

## Test
Prepare s file in remote SFTP server. And you can see the message in VANTIQ.
