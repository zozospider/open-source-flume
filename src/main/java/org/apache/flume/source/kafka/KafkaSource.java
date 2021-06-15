/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.kafka;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.zk.KafkaZkClient;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.shared.kafka.KafkaSSLUtil;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import static org.apache.flume.source.kafka.KafkaSourceConstants.*;

import scala.Option;
import scala.collection.JavaConverters;

/**
 * A Source for Kafka which reads messages from kafka topics.
 *
 * <tt>kafka.bootstrap.servers: </tt> A comma separated list of host:port pairs
 * to use for establishing the initial connection to the Kafka cluster.
 * For example host1:port1,host2:port2,...
 * <b>Required</b> for kafka.
 * <p>
 * <tt>kafka.consumer.group.id: </tt> the group ID of consumer group. <b>Required</b>
 * <p>
 * <tt>kafka.topics: </tt> the topic list separated by commas to consume messages from.
 * <b>Required</b>
 * <p>
 * <tt>maxBatchSize: </tt> Maximum number of messages written to Channel in one
 * batch. Default: 1000
 * <p>
 * <tt>maxBatchDurationMillis: </tt> Maximum number of milliseconds before a
 * batch (of any size) will be written to a channel. Default: 1000
 * <p>
 * <tt>kafka.consumer.*: </tt> Any property starting with "kafka.consumer" will be
 * passed to the kafka consumer So you can use any configuration supported by Kafka 0.9.0.X
 * <tt>useFlumeEventFormat: </tt> Reads events from Kafka Topic as an Avro FlumeEvent. Used
 * in conjunction with useFlumeEventFormat (Kafka Sink) or parseAsFlumeEvent (Kafka Channel)
 * <p>
 *
 *
 * 从 kafka topics 读取消息的 Kafka Source.
 *
 * kafka.bootstrap.servers: 一个逗号分隔的 host:port 对列表, 用于建立到 Kafka 集群的初始连接.
 *                          例如 port1,host2:port2,... 这是 kafka 必填的.
 *
 * kafka.consumer.group.id: 消费者组的 ID. 必填.
 *
 * kafka.topics: 以逗号分隔的 topic 列表, 用于从中消费消息. 必填.
 *
 * maxBatchSize: 一批中写入 Channel 的最大消息数. 默认值: 1000
 *
 * maxBatchDurationMillis: 将批次 (任何大小) 写入 Channel 之前的最大毫秒数. 默认值: 1000
 *
 * kafka.consumer.*: 任何以 "kafka.consumer" 开头的属性都会被传递给 kafka consumer, 所以你可以使用 Kafka 0.9.0.X 支持的任何配置.
 *
 * useFlumeEventFormat: 从 Kafka Topic 读取 events 作为 Avro FlumeEvent.
 *                      与 useFlumeEventFormat (Kafka Sink) 或 parseAsFlumeEvent (Kafka Channel) 结合使用
 */
// tips from: http://flume.apache.org/releases/content/1.9.0/FlumeUserGuide.html#kafka-source
// 目前支持 Kafka 服务器版本 0.10.1.0 或更高版本. 测试一直进行到 2.0.1, 这是发布时可用的最高版本.
public class KafkaSource extends AbstractPollableSource
        implements Configurable, BatchSizeSupported {
    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);

    // Constants used only for offset migration zookeeper connections
    private static final int ZK_SESSION_TIMEOUT = 30000;
    private static final int ZK_CONNECTION_TIMEOUT = 30000;

    private Context context;

    // KafkaConsumer 的 Properties 参数
    private Properties kafkaProps;
    private KafkaSourceCounter counter;

    // KafkaConsumer
    private KafkaConsumer<String, byte[]> consumer;
    private Iterator<ConsumerRecord<String, byte[]>> it;

    private final List<Event> eventList = new ArrayList<Event>();
    private Map<TopicPartition, OffsetAndMetadata> tpAndOffsetMetadata;
    private AtomicBoolean rebalanceFlag;

    private Map<String, String> headers;

    private Optional<SpecificDatumReader<AvroFlumeEvent>> reader = Optional.absent();
    private BinaryDecoder decoder = null;

    private boolean useAvroEventFormat;

    private int batchUpperLimit;
    private int maxBatchDurationMillis;

    private Subscriber subscriber;

    private String zookeeperConnect;
    private String bootstrapServers;
    private String groupId = DEFAULT_GROUP_ID;
    @Deprecated
    private boolean migrateZookeeperOffsets = DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS;
    private String topicHeader = null;
    private boolean setTopicHeader;

    @Override
    public long getBatchSize() {
        return batchUpperLimit;
    }

    /**
     * This class is a helper to subscribe for topics by using
     * different strategies
     *
     * 这个类是使用不同策略订阅 topics 的 helper
     */
    public abstract class Subscriber<T> {
        // 订阅 topics
        public abstract void subscribe(KafkaConsumer<?, ?> consumer, SourceRebalanceListener listener);

        public T get() {
            return null;
        }
    }

    // 继承 Subscriber
    private class TopicListSubscriber extends Subscriber<List<String>> {
        // 保存订阅的 topics 列表
        private List<String> topicList;

        public TopicListSubscriber(String commaSeparatedTopics) {
            this.topicList = Arrays.asList(commaSeparatedTopics.split("^\\s+|\\s*,\\s*|\\s+$"));
        }

        // 订阅 topics
        @Override
        public void subscribe(KafkaConsumer<?, ?> consumer, SourceRebalanceListener listener) {
            consumer.subscribe(topicList, listener);
        }

        @Override
        public List<String> get() {
            return topicList;
        }
    }

    private class PatternSubscriber extends Subscriber<Pattern> {
        private Pattern pattern;

        public PatternSubscriber(String regex) {
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public void subscribe(KafkaConsumer<?, ?> consumer, SourceRebalanceListener listener) {
            consumer.subscribe(pattern, listener);
        }

        @Override
        public Pattern get() {
            return pattern;
        }
    }


    @Override
    protected Status doProcess() throws EventDeliveryException {
        final String batchUUID = UUID.randomUUID().toString();
        String kafkaKey;
        Event event;
        byte[] eventBody;

        try {
            // prepare time variables for new batch
            final long nanoBatchStartTime = System.nanoTime();
            final long batchStartTime = System.currentTimeMillis();
            final long maxBatchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;

            // 从 KafkaConsumer 中拉取的多个 ConsumerRecords 转换成 event 并保存到 eventList 中
            // 后续在 eventList 达到阈值后会批量添加到 Channel 中
            while (eventList.size() < batchUpperLimit &&
                    System.currentTimeMillis() < maxBatchEndTime) {

                // 迭代器为空, 则继续从 Kafka 中拉取一批 ConsumerRecords 消息
                if (it == null || !it.hasNext()) {
                    // Obtaining new records
                    // Poll time is remainder time for current batch.
                    long durMs = Math.max(0L, maxBatchEndTime - System.currentTimeMillis());
                    Duration duration = Duration.ofMillis(durMs);

                    // 通过 KafkaConsumer.poll() 从 Kafka 中拉取一批 ConsumerRecords 消息, 保存到迭代器
                    ConsumerRecords<String, byte[]> records = consumer.poll(duration);
                    it = records.iterator();

                    // this flag is set to true in a callback when some partitions are revoked.
                    // If there are any records we commit them.
                    if (rebalanceFlag.compareAndSet(true, false)) {
                        break;
                    }
                    // check records after poll
                    if (!it.hasNext()) {
                        counter.incrementKafkaEmptyCount();
                        log.debug("Returning with backoff. No more data to read");
                        // batch time exceeded
                        break;
                    }
                }

                // 迭代器不为空, 则取出迭代器中的一条 ConsumerRecord 消息进行处理

                // get next message
                ConsumerRecord<String, byte[]> message = it.next();
                kafkaKey = message.key();

                // 构建 event body:
                // 将 Kafka 的 ConsumerRecord 的 value 转换成 Flume 的 event 的 body
                if (useAvroEventFormat) {
                    //Assume the event is in Avro format using the AvroFlumeEvent schema
                    //Will need to catch the exception if it is not
                    ByteArrayInputStream in =
                            new ByteArrayInputStream(message.value());
                    decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
                    if (!reader.isPresent()) {
                        reader = Optional.of(
                                new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class));
                    }
                    //This may throw an exception but it will be caught by the
                    //exception handler below and logged at error
                    AvroFlumeEvent avroevent = reader.get().read(null, decoder);

                    eventBody = avroevent.getBody().array();
                    headers = toStringMap(avroevent.getHeaders());
                } else {
                    eventBody = message.value();
                    headers.clear();
                    headers = new HashMap<String, String>(4);
                }

                // 构建 event headers:
                // 将 ConsumerRecord 中的 topic, partition, key 等信息添加到 event 的 headers 中

                // Add headers to event (timestamp, topic, partition, key) only if they don't exist
                // 向 event 添加 headers (时间戳, topic, partition, key) (仅当它们不存在时)
                if (!headers.containsKey(KafkaSourceConstants.TIMESTAMP_HEADER)) {
                    headers.put(KafkaSourceConstants.TIMESTAMP_HEADER,
                            String.valueOf(System.currentTimeMillis()));
                }
                // Only set the topic header if setTopicHeader and it isn't already populated
                if (setTopicHeader && !headers.containsKey(topicHeader)) {
                    headers.put(topicHeader, message.topic());
                }
                if (!headers.containsKey(KafkaSourceConstants.PARTITION_HEADER)) {
                    headers.put(KafkaSourceConstants.PARTITION_HEADER,
                            String.valueOf(message.partition()));
                }
                if (!headers.containsKey(OFFSET_HEADER)) {
                    headers.put(OFFSET_HEADER,
                            String.valueOf(message.offset()));
                }

                if (kafkaKey != null) {
                    headers.put(KafkaSourceConstants.KEY_HEADER, kafkaKey);
                }

                if (log.isTraceEnabled()) {
                    if (LogPrivacyUtil.allowLogRawData()) {
                        log.trace("Topic: {} Partition: {} Message: {}", new String[]{
                                message.topic(),
                                String.valueOf(message.partition()),
                                new String(eventBody)
                        });
                    } else {
                        log.trace("Topic: {} Partition: {} Message arrived.",
                                message.topic(),
                                String.valueOf(message.partition()));
                    }
                }

                // 通过 body, headers 构建 event
                // 并将 event 添加到 eventList 中, 方便后续批量处理
                event = EventBuilder.withBody(eventBody, headers);
                eventList.add(event);

                if (log.isDebugEnabled()) {
                    log.debug("Waited: {} ", System.currentTimeMillis() - batchStartTime);
                    log.debug("Event #: {}", eventList.size());
                }

                // For each partition store next offset that is going to be read.
                // 对于每个 partition 存储将要读取的下一个 offset.

                // 此 tpAndOffsetMetadata Map 变量保存每个分区消费到了哪个 offset.
                // key: TopicPartition (当前消息的 topic-partition)
                // value: OffsetAndMetadata (当前消息的 offset + 1)
                tpAndOffsetMetadata.put(new TopicPartition(message.topic(), message.partition()),
                        new OffsetAndMetadata(message.offset() + 1, batchUUID));
            }

            // eventList 达到阈值后批量添加到 Channel 中, 并处理 KafkaConsumer 手动控制 offset 的 commit 逻辑
            if (eventList.size() > 0) {
                counter.addToKafkaEventGetTimer((System.nanoTime() - nanoBatchStartTime) / (1000 * 1000));
                counter.addToEventReceivedCount((long) eventList.size());

                // 通过 ChannelProcessor 批量将 eventList 添加到 Channel 中
                getChannelProcessor().processEventBatch(eventList);

                counter.addToEventAcceptedCount(eventList.size());
                if (log.isDebugEnabled()) {
                    log.debug("Wrote {} events to channel", eventList.size());
                }

                // 清空 eventList 方便当前 doProcess() 方法下次使用
                eventList.clear();

                // KafkaConsumer 手动控制 offset 的 commit 逻辑
                if (!tpAndOffsetMetadata.isEmpty()) {
                    long commitStartTime = System.nanoTime();

                    // 因为 tpAndOffsetMetadata 记录了已经处理的 ConsumerRecords 中每个消息对应的不同分区消费到了哪个 offset
                    // 所以在此处提交 offset, 方便下次 poll()
                    consumer.commitSync(tpAndOffsetMetadata);
                    long commitEndTime = System.nanoTime();
                    counter.addToKafkaCommitTimer((commitEndTime - commitStartTime) / (1000 * 1000));
                    tpAndOffsetMetadata.clear();
                }

                // 处理 eventList 成功, 返回 READY 状态
                return Status.READY;
            }

            // eventList 没有数据, 返回 BACKOFF 状态
            return Status.BACKOFF;
        } catch (Exception e) {
            log.error("KafkaSource EXCEPTION, {}", e);
            counter.incrementEventReadOrChannelFail(e);

            // 处理异常, 返回 BACKOFF 状态
            return Status.BACKOFF;
        }
    }

    /**
     * We configure the source and generate properties for the Kafka Consumer
     *
     * Kafka Consumer properties are generated as follows:
     * 1. Generate a properties object with some static defaults that can be
     * overridden if corresponding properties are specified
     * 2. We add the configuration users added for Kafka (parameters starting
     * with kafka.consumer and must be valid Kafka Consumer properties
     * 3. Add source level properties (with no prefix)
     * @param context
     */
    @Override
    protected void doConfigure(Context context) throws FlumeException {
        this.context = context;
        headers = new HashMap<String, String>(4);
        tpAndOffsetMetadata = new HashMap<TopicPartition, OffsetAndMetadata>();
        rebalanceFlag = new AtomicBoolean(false);
        kafkaProps = new Properties();

        // can be removed in the next release
        // See https://issues.apache.org/jira/browse/FLUME-2896
        translateOldProperties(context);

        String topicProperty = context.getString(KafkaSourceConstants.TOPICS_REGEX);
        if (topicProperty != null && !topicProperty.isEmpty()) {
            // create subscriber that uses pattern-based subscription
            subscriber = new PatternSubscriber(topicProperty);
        } else if ((topicProperty = context.getString(KafkaSourceConstants.TOPICS)) != null &&
                !topicProperty.isEmpty()) {
            // create subscriber that uses topic list subscription
            subscriber = new TopicListSubscriber(topicProperty);
        } else if (subscriber == null) {
            throw new ConfigurationException("At least one Kafka topic must be specified.");
        }

        batchUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_SIZE,
                KafkaSourceConstants.DEFAULT_BATCH_SIZE);
        maxBatchDurationMillis = context.getInteger(KafkaSourceConstants.BATCH_DURATION_MS,
                KafkaSourceConstants.DEFAULT_BATCH_DURATION);

        useAvroEventFormat = context.getBoolean(KafkaSourceConstants.AVRO_EVENT,
                KafkaSourceConstants.DEFAULT_AVRO_EVENT);

        if (log.isDebugEnabled()) {
            log.debug(KafkaSourceConstants.AVRO_EVENT + " set to: {}", useAvroEventFormat);
        }

        zookeeperConnect = context.getString(ZOOKEEPER_CONNECT_FLUME_KEY);
        migrateZookeeperOffsets = context.getBoolean(MIGRATE_ZOOKEEPER_OFFSETS,
                DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS);

        bootstrapServers = context.getString(KafkaSourceConstants.BOOTSTRAP_SERVERS);
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            if (zookeeperConnect == null || zookeeperConnect.isEmpty()) {
                throw new ConfigurationException("Bootstrap Servers must be specified");
            } else {
                // For backwards compatibility look up the bootstrap from zookeeper
                log.warn("{} is deprecated. Please use the parameter {}",
                        KafkaSourceConstants.ZOOKEEPER_CONNECT_FLUME_KEY,
                        KafkaSourceConstants.BOOTSTRAP_SERVERS);

                // Lookup configured security protocol, just in case its not default
                String securityProtocolStr =
                        context.getSubProperties(KafkaSourceConstants.KAFKA_CONSUMER_PREFIX)
                                .get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
                if (securityProtocolStr == null || securityProtocolStr.isEmpty()) {
                    securityProtocolStr = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL;
                }
                bootstrapServers =
                        lookupBootstrap(zookeeperConnect, SecurityProtocol.valueOf(securityProtocolStr));
            }
        }

        String groupIdProperty =
                context.getString(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG);
        if (groupIdProperty != null && !groupIdProperty.isEmpty()) {
            groupId = groupIdProperty; // Use the new group id property
        }

        if (groupId == null || groupId.isEmpty()) {
            groupId = DEFAULT_GROUP_ID;
            log.info("Group ID was not specified. Using {} as the group id.", groupId);
        }

        setTopicHeader = context.getBoolean(KafkaSourceConstants.SET_TOPIC_HEADER,
                KafkaSourceConstants.DEFAULT_SET_TOPIC_HEADER);

        topicHeader = context.getString(KafkaSourceConstants.TOPIC_HEADER,
                KafkaSourceConstants.DEFAULT_TOPIC_HEADER);

        // 设置 KafkaConsumer 的 Properties 参数
        setConsumerProps(context);

        if (log.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
            log.debug("Kafka consumer properties: {}", kafkaProps);
        }

        if (counter == null) {
            counter = new KafkaSourceCounter(getName());
        }
    }

    // We can remove this once the properties are officially deprecated
    private void translateOldProperties(Context ctx) {
        // topic
        String topic = context.getString(KafkaSourceConstants.TOPIC);
        if (topic != null && !topic.isEmpty()) {
            subscriber = new TopicListSubscriber(topic);
            log.warn("{} is deprecated. Please use the parameter {}",
                    KafkaSourceConstants.TOPIC, KafkaSourceConstants.TOPICS);
        }

        // old groupId
        groupId = ctx.getString(KafkaSourceConstants.OLD_GROUP_ID);
        if (groupId != null && !groupId.isEmpty()) {
            log.warn("{} is deprecated. Please use the parameter {}",
                    KafkaSourceConstants.OLD_GROUP_ID,
                    KafkaSourceConstants.KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG);
        }
    }

    // 设置 KafkaConsumer 的 Properties 参数
    private void setConsumerProps(Context ctx) {
        kafkaProps.clear();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                KafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER);
        //Defaults overridden based on config
        kafkaProps.putAll(ctx.getSubProperties(KafkaSourceConstants.KAFKA_CONSUMER_PREFIX));
        //These always take precedence over config
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (groupId != null) {
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                KafkaSourceConstants.DEFAULT_AUTO_COMMIT);

        KafkaSSLUtil.addGlobalSSLParameters(kafkaProps);
    }

    /**
     * Generates the Kafka bootstrap connection string from the metadata stored in Zookeeper.
     * Allows for backwards compatibility of the zookeeperConnect configuration.
     */
    private String lookupBootstrap(String zookeeperConnect, SecurityProtocol securityProtocol) {
        try (KafkaZkClient zkClient = KafkaZkClient.apply(zookeeperConnect,
                JaasUtils.isZkSecurityEnabled(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, 10,
                Time.SYSTEM, "kafka.server", "SessionExpireListener")) {
            List<Broker> brokerList =
                    JavaConverters.seqAsJavaListConverter(zkClient.getAllBrokersInCluster()).asJava();
            List<BrokerEndPoint> endPoints = brokerList.stream()
                    .map(broker -> broker.brokerEndPoint(
                            ListenerName.forSecurityProtocol(securityProtocol))
                    )
                    .collect(Collectors.toList());
            List<String> connections = new ArrayList<>();
            for (BrokerEndPoint endPoint : endPoints) {
                connections.add(endPoint.connectionString());
            }
            return StringUtils.join(connections, ',');
        }
    }

    @VisibleForTesting
    String getBootstrapServers() {
        return bootstrapServers;
    }

    Properties getConsumerProps() {
        return kafkaProps;
    }

    /**
     * Helper function to convert a map of CharSequence to a map of String.
     */
    private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
        Map<String, String> stringMap = new HashMap<String, String>();
        for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
            stringMap.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return stringMap;
    }

    <T> Subscriber<T> getSubscriber() {
        return subscriber;
    }

    @Override
    protected void doStart() throws FlumeException {
        log.info("Starting {}...", this);

        // As a migration step check if there are any offsets from the group stored in kafka
        // If not read them from Zookeeper and commit them to Kafka
        // 作为迁移步骤, 检查存储在 kafka 中的组是否有任何 offsets
        // 如果没有从 Zookeeper 读取它们并将它们提交给 Kafka
        if (migrateZookeeperOffsets && zookeeperConnect != null && !zookeeperConnect.isEmpty()) {
            // For simplicity we only support migration of a single topic via the TopicListSubscriber.
            // There was no way to define a list of topics or a pattern in the previous Flume version.
            if (subscriber instanceof TopicListSubscriber &&
                    ((TopicListSubscriber) subscriber).get().size() == 1) {
                String topicStr = ((TopicListSubscriber) subscriber).get().get(0);

                // 获取 Kafka 保存的 topic 的消费 offset, 如果存在则返回, 如果不存在则从 ZooKeeper 中获取, 并更新到 Kafka 中
                migrateOffsets(topicStr);
            } else {
                log.info("Will not attempt to migrate offsets " +
                        "because multiple topics or a pattern are defined");
            }
        }

        //initialize a consumer.
        // 初始化一个 KafkaConsumer
        consumer = new KafkaConsumer<String, byte[]>(kafkaProps);

        // Subscribe for topics by already specified strategy
        // 通过已经指定的策略订阅 topics
        subscriber.subscribe(consumer, new SourceRebalanceListener(rebalanceFlag));

        log.info("Kafka source {} started.", getName());
        counter.start();
    }

    @Override
    protected void doStop() throws FlumeException {
        if (consumer != null) {
            consumer.wakeup();
            consumer.close();
        }
        if (counter != null) {
            counter.stop();
        }
        log.info("Kafka Source {} stopped. Metrics: {}", getName(), counter);
    }

    // 获取 Kafka 保存的 topic 的消费 offset, 如果存在则返回, 如果不存在则从 ZooKeeper 中获取, 并提交到 Kafka 中
    private void migrateOffsets(String topicStr) {
        try (KafkaZkClient zkClient = KafkaZkClient.apply(zookeeperConnect,
                JaasUtils.isZkSecurityEnabled(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, 10,
                Time.SYSTEM, "kafka.server", "SessionExpireListener");
             KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(kafkaProps)) {

            // 获取 Kafka 保存的 topic 的消费 offset, 如果存在则返回
            Map<TopicPartition, OffsetAndMetadata> kafkaOffsets =
                    getKafkaOffsets(consumer, topicStr);
            if (!kafkaOffsets.isEmpty()) {
                log.info("Found Kafka offsets for topic " + topicStr +
                        ". Will not migrate from zookeeper");
                log.debug("Offsets found: {}", kafkaOffsets);
                return;
            }

            // 如果 Kafka 中不存在 topic 的消费 offset, 则从 ZooKeeper 中获取, 并提交到 Kafka 中
            log.info("No Kafka offsets found. Migrating zookeeper offsets");
            Map<TopicPartition, OffsetAndMetadata> zookeeperOffsets =
                    getZookeeperOffsets(zkClient, consumer, topicStr);
            if (zookeeperOffsets.isEmpty()) {
                log.warn("No offsets to migrate found in Zookeeper");
                return;
            }

            log.info("Committing Zookeeper offsets to Kafka");
            log.debug("Offsets to commit: {}", zookeeperOffsets);
            // 将 Zookeeper 保存的 offset 提交到 Kafka 中
            consumer.commitSync(zookeeperOffsets);
            // Read the offsets to verify they were committed
            Map<TopicPartition, OffsetAndMetadata> newKafkaOffsets =
                    getKafkaOffsets(consumer, topicStr);
            log.debug("Offsets committed: {}", newKafkaOffsets);
            if (!newKafkaOffsets.keySet().containsAll(zookeeperOffsets.keySet())) {
                throw new FlumeException("Offsets could not be committed");
            }
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getKafkaOffsets(
            KafkaConsumer<String, byte[]> client, String topicStr) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        List<PartitionInfo> partitions = client.partitionsFor(topicStr);
        for (PartitionInfo partition : partitions) {
            TopicPartition key = new TopicPartition(topicStr, partition.partition());
            OffsetAndMetadata offsetAndMetadata = client.committed(key);
            if (offsetAndMetadata != null) {
                offsets.put(key, offsetAndMetadata);
            }
        }
        return offsets;
    }

    private Map<TopicPartition, OffsetAndMetadata> getZookeeperOffsets(
            KafkaZkClient zkClient, KafkaConsumer<String, byte[]> consumer, String topicStr) {

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        List<PartitionInfo> partitions = consumer.partitionsFor(topicStr);
        for (PartitionInfo partition : partitions) {
            TopicPartition topicPartition = new TopicPartition(topicStr, partition.partition());
            Option<Object> optionOffset = zkClient.getConsumerOffset(groupId, topicPartition);
            if (optionOffset.nonEmpty()) {
                Long offset = (Long) optionOffset.get();
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
                offsets.put(topicPartition, offsetAndMetadata);
            }
        }
        return offsets;
    }
}

class SourceRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(SourceRebalanceListener.class);
    private AtomicBoolean rebalanceFlag;

    public SourceRebalanceListener(AtomicBoolean rebalanceFlag) {
        this.rebalanceFlag = rebalanceFlag;
    }

    // Set a flag that a rebalance has occurred. Then commit already read events to kafka.
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            log.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
            rebalanceFlag.set(true);
        }
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            log.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
        }
    }
}
