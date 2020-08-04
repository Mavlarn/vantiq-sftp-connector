package io.vantiq.ext.sftp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SFTPConnectorApplication {

    public static void main(String[] args) {

//        Map<String, String> connectInfo = constructConfig();
//        SFTPConnector SFTPConnector = new SFTPConnector(connectInfo.get(VANTIQ_SOURCE_NAME), connectInfo);
//        SFTPConnector.start();

        SpringApplication.run(SFTPConnectorApplication.class, args);

    }

}
