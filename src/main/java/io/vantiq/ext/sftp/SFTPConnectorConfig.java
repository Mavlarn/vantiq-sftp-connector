package io.vantiq.ext.sftp;

import java.util.Map;

public class SFTPConnectorConfig {

    private int sftpServerPort;
    private String sftpServerHost;
    private String sftpUser;
    private String sftpPassword;
    private String remoteDir;
    private String logFreq;
    private String fetchInterval;
    private String sleepInterval;
//    "sftp_server_port": "22",
//            "sftp_user": "root",
//            "sftp_server_host": "39.100.118.122",
//            "sftp_password": "",
//            "remote_dir": "mt_test",
//            "log_freq": "50",
//            "fetch_interval": "5000",
//            "sleep_interval": "50"


    public SFTPConnectorConfig(Map<String, String> sourceConfig) {

    }

}
