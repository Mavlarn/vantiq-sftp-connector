package io.vantiq.ext.sftp.handler;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import io.vantiq.ext.sdk.ExtensionServiceMessage;
import io.vantiq.ext.sdk.Handler;
import io.vantiq.ext.sftp.SFTPConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.dsl.StandardIntegrationFlow;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore;
import org.springframework.integration.sftp.dsl.Sftp;
import org.springframework.integration.sftp.filters.SftpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.sftp.filters.SftpRegexPatternFileListFilter;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ConfigHandler extends Handler<ExtensionServiceMessage> {

    static final Logger LOG = LoggerFactory.getLogger(ConfigHandler.class);

    private static final String CONFIG = "config";
    private static final String SFTP_SERVER_HOST = "sftp_server_host";
    private static final String SFTP_SERVER_PORT = "sftp_server_port";
    private static final String SFTP_USER = "sftp_user";
    private static final String SFTP_PASSWORD = "sftp_password";
    private static final String REMOTE_DIR = "remote_dir";
    private static final String FILE_FILTER = "file_filter";


    private SFTPConnector connector;

    public ConfigHandler(SFTPConnector connector) {
        this.connector = connector;
    }

    /**
     *
     * @param message   A message to be handled
     */
    @Override
    public void handleMessage(ExtensionServiceMessage message) {
        LOG.info("Configuration for source:{}", message.getSourceName());
        Map<String, Object> configObject = (Map) message.getObject();
        Map<String, String> topicConfig;

        // Obtain entire config from the message object
        if ( !(configObject.get(CONFIG) instanceof Map)) {
            LOG.error("Configuration failed. No configuration suitable for SFTP Connector.");
            failConfig();
            return;
        }
        topicConfig = (Map) configObject.get(CONFIG);

        String sftpServer = topicConfig.get(SFTP_SERVER_HOST);
        String sftpPortStr = topicConfig.getOrDefault(SFTP_SERVER_PORT, "22");
        int sftpPort = Integer.parseInt(sftpPortStr);
        String sftpUser = topicConfig.get(SFTP_USER);
        String sftpPassword = topicConfig.get(SFTP_PASSWORD);
        final String remoteDir = topicConfig.get(REMOTE_DIR);
        final String fileFilter = topicConfig.get(FILE_FILTER);

        if (StringUtils.isEmpty(sftpServer)) {
            LOG.warn("No sftp server config for SFTP Connector.");
            return;
        }

        ApplicationContext context = this.connector.getContext();
        AutowireCapableBeanFactory registry = context.getAutowireCapableBeanFactory();



        try {
            CsvMapper mapper = new CsvMapper();
            mapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
            ObjectReader reader = mapper.readerFor(String[].class);


            DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
            factory.setHost(sftpServer);
            factory.setPort(sftpPort);
            factory.setUser(sftpUser);
            if (StringUtils.hasText(sftpPassword)) {
                factory.setPassword(sftpPassword);
            } else {
                factory.setPrivateKey(new ClassPathResource("key.pem")); // put key file in resource directory
            }
            factory.setAllowUnknownKeys(true);
            CachingSessionFactory sftpSessionFactory = new CachingSessionFactory<>(factory);
            sftpSessionFactory.setTestSession(true);
            connector.setSessionFactory(sftpSessionFactory);

            // register
//            registry.destroyBean();
            registry.initializeBean(sftpSessionFactory, "sftpSessionFactory");

            PropertiesPersistingMetadataStore metadataStore = new PropertiesPersistingMetadataStore();
            metadataStore.setBaseDirectory("sftp-data");
            metadataStore.setFileName("mt-metaStore");

            SftpPersistentAcceptOnceFileListFilter acceptOnceFilter = new SftpPersistentAcceptOnceFileListFilter(metadataStore, "mt_");
            acceptOnceFilter.setFlushOnUpdate(true);

            registry.initializeBean(metadataStore, "metadataStore");

            CompositeFileListFilter remoteFileFilter = new CompositeFileListFilter();
            remoteFileFilter.addFilter(acceptOnceFilter);
//            remoteFileFilter.addFilter(new SftpRegexPatternFileListFilter(".*\\.txt"));
            if (fileFilter != null) {
                remoteFileFilter.addFilter(new SftpRegexPatternFileListFilter(fileFilter));
            } else {
                remoteFileFilter.addFilter(new SftpRegexPatternFileListFilter(".*\\.DT"));
            }
            registry.initializeBean(remoteFileFilter, "remoteFileFilter");


            AtomicLong count = new AtomicLong();
            SftpRemoteFileTemplate template = new SftpRemoteFileTemplate(sftpSessionFactory);
            StandardIntegrationFlow flow = IntegrationFlows
                    .from(Sftp.inboundStreamingAdapter(template)
                              .filter(remoteFileFilter)
                              .remoteDirectory(remoteDir),
                            e -> e.id("sftpInboundAdapter")
                                  .autoStartup(true)
                                  .poller(Pollers.fixedDelay(5000)))
                    .handle(Files.splitter(true, true))
                    .handle(m -> {
                        // handle every line
                        Object payload = m.getPayload();
                        if (payload instanceof String) {
                            try {
                                String line = (String) payload;
                                if (StringUtils.isEmpty(line)) {
                                    return;
                                }
                                line = line.replace("||", ",");

                                String[] result = reader.readValue(line);
                                if (count.getAndIncrement() % 10000 == 0) {
                                    long theTime = System.currentTimeMillis();
                                    LOG.debug("line:{}\t{}", theTime, result);
                                }
                                // need to wrap as List, which can be identified as JSON list in VANTIQ.
                                Map data = new HashMap();
                                data.put("data", Arrays.asList(result));
                                connector.getVantiqClient().sendNotification(data);

                            } catch (Exception e) {
                                LOG.error("parse line error:" + payload, e);
                            }
                        }
                    })

                    .get();
            registry.initializeBean(flow, "sftpInboundAdapter");
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

    }

    /**
     * Closes the source {@link SFTPConnector} and marks the configuration as completed. The source will
     * be reactivated when the source reconnects, due either to a Reconnect message (likely created by an update to the
     * configuration document) or to the WebSocket connection crashing momentarily.
     */
    private void failConfig() {
//        connector.close();
    }

}
