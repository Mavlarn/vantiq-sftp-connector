package io.vantiq.ext.sftp.handler;

import io.vantiq.extjsdk.ExtensionServiceMessage;
import io.vantiq.extjsdk.Handler;
import io.vantiq.ext.sftp.SFTPConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconnectHandler extends Handler<ExtensionServiceMessage> {

    static final Logger LOG = LoggerFactory.getLogger(ReconnectHandler.class);

    private SFTPConnector connector;

    public ReconnectHandler(SFTPConnector connector) {
        this.connector = connector;
    }

    @Override
    public void handleMessage(ExtensionServiceMessage message) {

//        ExtensionWebSocketClient client = connector.getVantiqClient();
//        CompletableFuture<Boolean> success = client.connectToSource();
//
//        try {
//            if ( !success.get(ConnectorConstants.CONNECTOR_CONNECT_TIMEOUT, TimeUnit.SECONDS) ) {
//                if (!client.isOpen()) {
//                    LOG.error("Failed to connect to server url.");
//                } else if (!client.isAuthed()) {
//                    LOG.error("Failed to authenticate within 10 seconds using the given authentication data.");
//                } else {
//                    LOG.error("Failed to connect within 10 seconds");
//                }
//            }
//        } catch (InterruptedException | ExecutionException | TimeoutException e) {
//            LOG.error("Could not reconnect to source within 10 seconds: ", e);
//        }
    }
}
