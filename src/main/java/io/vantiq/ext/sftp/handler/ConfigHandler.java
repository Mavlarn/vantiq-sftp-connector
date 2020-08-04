package io.vantiq.ext.sftp.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.vantiq.ext.sftp.DeviceState;
import io.vantiq.ext.sftp.SFTPConnector;
import io.vantiq.extjsdk.ExtensionServiceMessage;
import io.vantiq.extjsdk.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.integration.aggregator.SimpleSequenceSizeReleaseStrategy;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.dsl.StandardIntegrationFlow;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.splitter.FileSplitter;
import org.springframework.integration.handler.advice.RateLimiterRequestHandlerAdvice;
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore;
import org.springframework.integration.sftp.dsl.Sftp;
import org.springframework.integration.sftp.filters.SftpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.sftp.filters.SftpRegexPatternFileListFilter;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
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
    private static final String FETCH_INTERVAL = "fetch_interval";
    private static final String RATE_LIMIT = "rate_limit";
    private static final String PACKAGE_SIZE = "package_size";
    private static final String OUTPUT_RESULT = "output_result";

    private SFTPConnector connector;

    public ConfigHandler(SFTPConnector connector) {
        this.connector = connector;
    }

    /**
     *
     * @param message   A message to be handled
     */
    @Override
    public void handleMessage(ExtensionServiceMessage configMessage) {
        LOG.info("Configuration for source:{}", configMessage.getSourceName());
        Map<String, Object> configObject = (Map) configMessage.getObject();
        Map<String, String> topicConfig;

        // Obtain entire config from the message object
        if ( !(configObject.get(CONFIG) instanceof Map)) {
            LOG.error("Configuration failed. No configuration suitable for SFTP Connector.");
            failConfig();
            return;
        }
        topicConfig = (Map) configObject.get(CONFIG);
        LOG.info("Connector config:{}", topicConfig);

        String sftpServer = topicConfig.get(SFTP_SERVER_HOST);
        String sftpPortStr = topicConfig.getOrDefault(SFTP_SERVER_PORT, "22");
        int sftpPort = Integer.parseInt(sftpPortStr);
        String sftpUser = topicConfig.get(SFTP_USER);
        String sftpPassword = topicConfig.get(SFTP_PASSWORD);
        final String remoteDir = topicConfig.get(REMOTE_DIR);
        final String fileFilter = topicConfig.get(FILE_FILTER);

        String fetchIntStr = topicConfig.getOrDefault(FETCH_INTERVAL, "10000");
        final int fetchInt = Integer.parseInt(fetchIntStr);
        String rateLimitStr = topicConfig.getOrDefault(RATE_LIMIT, "1000");
        final int rateLimit = Integer.parseInt(rateLimitStr);
        String packageSizeStr = topicConfig.getOrDefault(PACKAGE_SIZE, "10");
        final int packageSize = Integer.parseInt(packageSizeStr);

        boolean outputResult = Boolean.parseBoolean(topicConfig.get(OUTPUT_RESULT));

        if (StringUtils.isEmpty(sftpServer)) {
            LOG.error("No sftp server config for SFTP Connector.");
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
//            registry.initializeBean(sftpSessionFactory, "sftpSessionFactory");

            PropertiesPersistingMetadataStore metadataStore = new PropertiesPersistingMetadataStore();
            metadataStore.setBaseDirectory("metadata");
            metadataStore.setFileName("mt-metaStore");
            metadataStore.afterPropertiesSet();

            SftpPersistentAcceptOnceFileListFilter acceptOnceFilter = new SftpPersistentAcceptOnceFileListFilter(metadataStore, "mt_");
            acceptOnceFilter.setFlushOnUpdate(true);

            CompositeFileListFilter remoteFileFilter = new CompositeFileListFilter();
            remoteFileFilter.addFilter(acceptOnceFilter);
            SftpRegexPatternFileListFilter fileListFilter;
            if (fileFilter != null) {
                fileListFilter = new SftpRegexPatternFileListFilter(fileFilter);
            } else {
                fileListFilter = new SftpRegexPatternFileListFilter(".*\\.DT");
            }
            fileListFilter.setAlwaysAcceptDirectories(true);
            remoteFileFilter.addFilter(fileListFilter);

            // remoteFileFilter在close的时候会flush metaStore到文件
            registry.initializeBean(remoteFileFilter, "remoteFileFilter");
            SftpRemoteFileTemplate template = new SftpRemoteFileTemplate(sftpSessionFactory);


//            RecursiveDirectoryScanner scanner =
//            Sftp.inboundAdapter(sftpSessionFactory).scanner()

            Set deviceSet = new HashSet(50000);

            AtomicLong lineCount = new AtomicLong(0);
            AtomicLong packSentCount = new AtomicLong(0);
            AtomicLong fileCount = new AtomicLong(0);

            Map<String, DeviceState> accumulateState = new HashMap();
            List<DeviceState> deviceStateList = new ArrayList<>();

            StandardIntegrationFlow flow = IntegrationFlows
                    .from(Sftp.inboundStreamingAdapter(template)
                              .filter(remoteFileFilter)
                              .remoteDirectory(remoteDir),
                            e -> e.id("sftpInboundAdapter")
                                  .autoStartup(true)
                                  .poller(Pollers.fixedDelay(fetchInt)))
                    .handle(Files.splitter(true, true))
                    .filter(m -> {
                        if (m instanceof String) {
                            lineCount.incrementAndGet();
                            /****** calculate device start *****/
                            try {
                                String line = m.toString().replace("||", ",");
                                String[] result = reader.readValue(line);
                                List dataList = new ArrayList(3);
                                dataList.add(result[0]);
                                dataList.add(result[1]);
                                dataList.add(result[2]);
                                if (StringUtils.hasText(result[2])) {

                                    String deviceId = result[0];
                                    String dateTime = result[1];
                                    BigDecimal value = new BigDecimal(result[2]);

                                    DeviceState deviceState = accumulateState.get(deviceId);
                                    if (deviceState == null) {
                                        deviceState = new DeviceState(deviceId, dateTime);
                                        accumulateState.put(deviceId, deviceState);
                                        deviceStateList.add(deviceState);
                                    }
                                    deviceState.updateMean(value);

                                    if (value.compareTo(deviceState.getMaxValue()) > 0) {
                                        deviceState.setMaxValue(value);
                                        deviceState.setMaxTime(dateTime);
                                        deviceState.setUpdateTime(new Date());
                                    }
                                    if (value.compareTo(deviceState.getMinValue()) < 0) {
                                        deviceState.setMinValue(value);
                                        deviceState.setMinTime(dateTime);
                                        deviceState.setUpdateTime(new Date());
                                    }
                                }
                            } catch (Exception e) {
                                LOG.error(e.getMessage(), e);
                            }
                            /****** calculate device end *****/

                            return true;
                        } else if (m instanceof FileSplitter.FileMarker) {
                            FileSplitter.FileMarker marker = (FileSplitter.FileMarker)m;
                            if (marker.getMark() == FileSplitter.FileMarker.Mark.START) {
                                Map data = new HashMap();
                                Map summary = new HashMap();
                                data.put("summary", summary);
                                summary.put("file", marker.getFilePath());
                                connector.getVantiqClient().sendNotification(data);
                            } else if (marker.getMark() == FileSplitter.FileMarker.Mark.END) {
                                // End: FileMarker [filePath=mt_testFS_20200318_235521_TMR_1201001_1584547126001_2110.DT, mark=END, lineCount=5000]
                                Map data = new HashMap();
                                Map summary = new HashMap();
                                data.put("summary", summary);
                                summary.put("file", marker.getFilePath());
                                summary.put("lineCount", marker.getLineCount());
                                connector.getVantiqClient().sendNotification(data);

                                LOG.info("Device count: {}\t, line count:{}\tfor file: {}", deviceSet.size(),
                                        marker.getLineCount(), marker.getFilePath());
                                LOG.info("Processed file: {}", fileCount.incrementAndGet());

                                /****** output result start *****/
                                if (outputResult && fileCount.get() == 1 || fileCount.get() > 1035) {
                                    File output = new File("result.csv");
                                    try {
                                        CsvMapper outMapper = new CsvMapper();
                                        CsvSchema schema = outMapper.schemaFor(DeviceState.class).withHeader();
                                        outMapper.writer(schema).writeValues(output).writeAll(deviceStateList);
                                    } catch (IOException e) {
                                        LOG.error(e.getMessage(), e);
                                    }
                                }
                                /****** output result end *****/

                                deviceSet.clear();
                            }
                        }
                        return false;
                    },h -> {
                        RateLimiterConfig conf = RateLimiterConfig.custom()
                                                                  .limitRefreshPeriod(Duration.ofSeconds(1))
                                                                  .limitForPeriod(rateLimit)
                                                                  .build();
                        RateLimiterRequestHandlerAdvice limiter = new RateLimiterRequestHandlerAdvice(conf);
                        h.advice(limiter);
                    })
                    .transform(msg -> {
                        try {
                            String line = msg.toString().replace("||", ",");
                            String[] result = reader.readValue(line);
                            List dataList = new ArrayList(3);
                            dataList.add(result[0]);
                            dataList.add(result[1]);
                            dataList.add(result[2]);
                            if (StringUtils.hasText(result[2])) {
                                deviceSet.add(result[0]);
                            }
                            return dataList;
                        } catch (JsonProcessingException e) {
                            LOG.error(e.getMessage(), e);
                            return null;
                        }
                    })
                    .aggregate(m -> {
                        m.correlationStrategy(message -> {
                            if (message.getPayload() instanceof List) { // 已经转换成List
                                long currentCount = lineCount.get();
                                long groupIdCount = currentCount / packageSize;

                                String groupId = message.getHeaders().get("file_remoteFile").toString() + groupIdCount;
                                return groupId;  // 按文件名分组，
                            } else {
                                return "OTHER"; // separate group
                            }
                        })
                         .groupTimeout(10)
                         .sendPartialResultOnExpiry(true)
                         .releaseStrategy(new SimpleSequenceSizeReleaseStrategy());
                    })

                    .handle(m -> {
//                        m.getHeaders().get()
                        List aggregatedData = (List)m.getPayload();
//                        LOG.info("got aggregated data:{}", m);
                        LOG.info("Sent aggregated data count:{}, sent pack count:{}", aggregatedData.size(), packSentCount.incrementAndGet());

                        Map data = new HashMap();
                        data.put("data", m.getPayload());
                        connector.getVantiqClient().sendNotification(data);

                        if (lineCount.get() % 1000 == 0) {
                            LOG.info("Sending data...");
                        }
                    })
                    .get();
            connector.getFlowContext().registration(flow).register().getId();

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
