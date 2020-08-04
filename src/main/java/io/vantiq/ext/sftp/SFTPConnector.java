package io.vantiq.ext.sftp;

import io.vantiq.ext.sftp.handler.*;
import io.vantiq.extjsdk.ConnectorConfig;
import io.vantiq.extjsdk.ExtensionWebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.vantiq.extjsdk.ConnectorConstants.CONNECTOR_CONNECT_TIMEOUT;
import static io.vantiq.extjsdk.ConnectorConstants.RECONNECT_INTERVAL;


@Component
public class SFTPConnector implements Closeable {

    static final Logger LOG = LoggerFactory.getLogger(SFTPConnector.class);

    ExtensionWebSocketClient vantiqClient = null;
    String sourceName;
    String vantiqUrl;
    String vantiqToken;


    CachingSessionFactory sessionFactory;

    private ConnectorConfig connectionInfo;
    @Autowired
    private ApplicationContext context;
    @Autowired
    private IntegrationFlowContext flowContext;

    public SFTPConnector() { }

    @PostConstruct
    public void start() {
        connectionInfo = new ConnectorConfig();
        if (connectionInfo == null) {
            throw new RuntimeException("No VANTIQ connection information provided");
        }
        if (connectionInfo.getSourceName() == null) {
            throw new RuntimeException("No source name provided");
        }

        this.vantiqUrl = connectionInfo.getVantiqUrl();
        this.vantiqToken = connectionInfo.getToken();
        this.sourceName = connectionInfo.getSourceName();

        vantiqClient = new ExtensionWebSocketClient(sourceName);

        vantiqClient.setConfigHandler(new ConfigHandler(this));
        vantiqClient.setReconnectHandler(new ReconnectHandler(this));
        vantiqClient.setCloseHandler(new CloseHandler(this));
        vantiqClient.setPublishHandler(new PublishHandler(this));
        vantiqClient.setQueryHandler(new QueryHandler(this));

        boolean sourcesSucceeded = false;
        while (!sourcesSucceeded) {
            vantiqClient.initiateFullConnection(vantiqUrl, vantiqToken);

            sourcesSucceeded = checkConnectionFails(vantiqClient, CONNECTOR_CONNECT_TIMEOUT);
            if (!sourcesSucceeded) {
                try {
                    Thread.sleep(RECONNECT_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.error("An error occurred when trying to sleep the current thread. Error Message: ", e);
                }
            }
        }
    }

    @Override
    public void close() {
        this.vantiqClient.close();
    }

    public ExtensionWebSocketClient getVantiqClient() {
        return vantiqClient;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getVantiqUrl() {
        return vantiqUrl;
    }

    public String getVantiqToken() {
        return vantiqToken;
    }

    public ConnectorConfig getConnectionInfo() {
        return connectionInfo;
    }

    public ApplicationContext getContext() {
        return context;
    }

    public CachingSessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public void setSessionFactory(CachingSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public IntegrationFlowContext getFlowContext() {
        return flowContext;
    }

    public void setFlowContext(IntegrationFlowContext flowContext) {
        this.flowContext = flowContext;
    }

    /**
     * Waits for the connection to succeed or fail, logs and exits if the connection does not succeed within
     * {@code timeout} seconds.
     *
     * @param client    The client to watch for success or failure.
     * @param timeout   The maximum number of seconds to wait before assuming failure and stopping
     * @return          true if the connection succeeded, false if it failed to connect within {@code timeout} seconds.
     */
    public boolean checkConnectionFails(ExtensionWebSocketClient client, int timeout) {
        boolean sourcesSucceeded = false;
        try {
            sourcesSucceeded = client.getSourceConnectionFuture().get(timeout, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
            LOG.error("Timeout: full connection did not succeed within {} seconds: {}", timeout, e);
        }
        catch (Exception e) {
            LOG.error("Exception occurred while waiting for webSocket connection", e);
        }
        if (!sourcesSucceeded) {
            LOG.error("Failed to connect to all sources.");
            if (!client.isOpen()) {
                LOG.error("Failed to connect to server url '" + vantiqUrl + "'.");
            } else if (!client.isAuthed()) {
                LOG.error("Failed to authenticate within " + timeout + " seconds using the given authentication data.");
            } else {
                LOG.error("Failed to connect within 10 seconds");
            }
            return false;
        }
        return true;
    }
}
