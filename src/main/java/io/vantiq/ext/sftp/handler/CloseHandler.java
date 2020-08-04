package io.vantiq.ext.sftp.handler;

import io.vantiq.extjsdk.ExtensionWebSocketClient;
import io.vantiq.extjsdk.Handler;
import io.vantiq.ext.sftp.SFTPConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseHandler extends Handler<ExtensionWebSocketClient> {

    static final Logger LOG = LoggerFactory.getLogger(CloseHandler.class);

    private SFTPConnector connector;

    public CloseHandler(SFTPConnector connector) {
        this.connector = connector;
    }

    @Override
    public void handleMessage(ExtensionWebSocketClient client) {

        LOG.info("Close handler: {}", client);
//        SimpleMessageListenerContainer listener = this.connector.getMqListener();
//        listener.stop();
//        LOG.info("Stopped listener container:{}", listener);
//        connector.getSessionFactory().destroy();

        // reconnect
//        boolean sourcesSucceeded = false;
//        while (!sourcesSucceeded) {
//            client.initiateFullConnection(connector.getVantiqUrl(), connector.getVantiqToken());
//            sourcesSucceeded = connector.checkConnectionFails(client, CONNECTOR_CONNECT_TIMEOUT);
//            if (!sourcesSucceeded) {
//                try {
//                    Thread.sleep(RECONNECT_INTERVAL);
//                } catch (InterruptedException e) {
//                    LOG.error("An error occurred when trying to sleep the current thread. Error Message: ", e);
//                }
//            }
//        }


    }
}
