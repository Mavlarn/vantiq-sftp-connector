package io.vantiq.ext.sftp.handler;

import io.vantiq.ext.sftp.SFTPConnector;
import io.vantiq.ext.sdk.ExtensionServiceMessage;
import io.vantiq.ext.sdk.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryHandler extends Handler<ExtensionServiceMessage> {

    static final Logger LOG = LoggerFactory.getLogger(QueryHandler.class);

    private SFTPConnector connector;

    public QueryHandler(SFTPConnector connector) {
        this.connector = connector;
    }

    @Override
    public void handleMessage(ExtensionServiceMessage msg) {
        LOG.warn("SFTP not support query");
    }
}
