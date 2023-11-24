package win.delin.test;

import com.alibaba.fastjson.JSONObject;
import com.revinate.guava.util.concurrent.RateLimiter;
import okio.Buffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 消费者线程
 */
public class UserKafkaProducer extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(UserKafkaProducer.class);

    private KafkaProducer<byte[], byte[]> producer;
    private String topic;
    private String sendType;
    private String sourceType;
    private int days;
    private final ProducerCallBack callBack;

    private final List<Object> dataList;

    private int[] partitions;

    private int currentIndex;

    private final RateLimiter rateLimiter;

    public UserKafkaProducer(Properties properties, List<String> dataList, double permitRate) throws IOException {
        buildProducer(properties);
        callBack = new ProducerCallBack();
        this.dataList = new ArrayList<>(dataList);
        dataList.clear();
        this.rateLimiter = RateLimiter.create(permitRate);
    }

    private void buildProducer(Properties properties) throws IOException {
        boolean kerberosEnable = Boolean.parseBoolean(properties.getOrDefault("kerberos.enable", "false").toString());

        Properties props = new Properties();
        if (kerberosEnable) {
            // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
            String securityProtocol = "security.protocol";
            props.put(securityProtocol, properties.getOrDefault(securityProtocol, "SASL_PLAINTEXT"));
            // 服务名
            String saslKerberosServiceName = "sasl.kerberos.service.name";
            props.put(saslKerberosServiceName, "kafka");
            // 域名
            String kerberosDomainName = "kerberos.domain.name";
            props.put(kerberosDomainName, properties.getOrDefault(kerberosDomainName, "hadoop.hadoop.com"));

            String userDir = Thread.currentThread().getContextClassLoader().getResource("").getPath();
            String krbFile = userDir + "krb5.conf";
            String userKeyTableFile = userDir + "user.keytab";

            if (!new File(krbFile).exists()) {
                logger.error("缺失文件：{}", krbFile);
                throw new RuntimeException("缺失文件：" + krbFile);
            }

            if (!new File(userKeyTableFile).exists()) {
                logger.error("缺失文件：{}", userKeyTableFile);
                throw new RuntimeException("缺失文件：" + userKeyTableFile);
            }

            //windows路径下分隔符替换
            userKeyTableFile = userKeyTableFile.replace("\\", "\\\\");
            krbFile = krbFile.replace("\\", "\\\\");

            String principal = properties.getProperty("principal");
            if (StringUtils.isBlank(principal)) {
                logger.error("缺失配置：principal");
                throw new RuntimeException("缺失配置：principal");
            }

            LoginUtil.setKrb5Config(krbFile);
            LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
            LoginUtil.setJaasFile(principal, userKeyTableFile);
        }

        String brokerListStr = properties.getProperty("brokers");
        String partitionType = properties.getProperty("partitionType");

        this.topic = properties.getProperty("topic");
        this.sourceType = properties.getProperty("sourceType");
        this.days = Integer.parseInt(properties.getProperty("minusDay"));

        this.sendType = properties.getProperty("sendType");

        props.put("metadata.broker.list", brokerListStr);
        props.put("bootstrap.servers", brokerListStr);
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 10000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("compression.type", "snappy");
        if ("random".equals(partitionType)) {
            props.put("partitioner.class", "win.delin.test.RandomPartitioner");
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String)entry.getKey();
            if (key.startsWith("kafka_")){
                props.put(key.replace("kafka_",""), entry.getValue());
            }
        }

        logger.info(props.toString());

        producer = new KafkaProducer<>(props);

        List<PartitionInfo> partitionInfos = new ArrayList<>(producer.partitionsFor(topic));
        partitionInfos.sort(Comparator.comparingInt(PartitionInfo::partition));
        partitions = new int[partitionInfos.size()];
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = partitionInfos.get(i).partition();
        }
        logger.info("topic: {}, partition: {}", topic, partitions);
    }

    @Override
    public void run() {
        if ("json".equals(sourceType)){
            FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
            boolean first = true;
            while (!Thread.interrupted()) {
                if (first) {
                    for (int i = 0; i < dataList.size(); i++) {
                        String data = (String) dataList.get(i);
                        JSONObject dataJson = JSONObject.parseObject(data);
                        String newTime = format.format(LocalDateTime.now().minus(days, ChronoUnit.DAYS).toInstant(ZoneOffset.of("+8")).toEpochMilli());
                        dataJson.put("collectorReceiptTime", newTime);
                        dataJson.put("startTime", newTime);
                        dataJson.put("eventId", DistributeAtomicUniqueIdGenerator.getInstance().generateUniqueEventId());
                        send(dataJson.toJSONString().getBytes(StandardCharsets.UTF_8));
                        dataJson.remove("collectorReceiptTime");
                        dataJson.remove("startTime");
                        dataJson.remove("eventId");
                        String removed = dataJson.toJSONString();
                        dataList.set(i, removed.substring(0, removed.length() -1).getBytes(StandardCharsets.UTF_8));
                    }
                    logger.info("第一次数据发送完成，数据结构转换完成，总条数：{}", dataList.size());
                } else {
                    for (Object data : dataList) {
                        byte[] dataBytes = (byte[]) data;
                        okio.Buffer buffer = new Buffer();
                        buffer.write(dataBytes);
                        String newTime = format.format(LocalDateTime.now().minus(days, ChronoUnit.DAYS).toInstant(ZoneOffset.of("+8")).toEpochMilli());
                        buffer.writeUtf8(",\"collectorReceiptTime\":\"")
                                .writeUtf8(newTime)
                                .writeUtf8("\",\"startTime\":\"").writeUtf8(newTime)
                                .writeUtf8("\",\"eventId\":\"")
                                .writeUtf8(String.valueOf(DistributeAtomicUniqueIdGenerator.getInstance().generateUniqueEventId()))
                                .writeUtf8("\"}");
                        send(buffer.readByteArray());
                    }
                }
                first = false;
            }
        } else {
            while (!Thread.interrupted()) {
                for (Object data : this.dataList) {
                    send(((String)data).getBytes(StandardCharsets.UTF_8));
                }
            }
        }

    }

    private void send(byte[] data) {
        //非阻塞方式，如果获取到许可发送数据否则过掉 ？？
        rateLimiter.acquire();
        if ("async".equals(sendType)){
            asyncSend(data);
        } else if("sync".equals(sendType)) {
            syncSend(data);
        } else {
            onlySend(data);
        }
    }

    /**
     * 只发消息不确认
     * @param data
     */
    private void onlySend(byte[] data) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, loopGetPartitionId(), null, data);
        producer.send(record);
        MockDataFromFileToKafka.TOTAL.add(1L);
    }

    /**
     * 同步发且确认
     * 保证消息顺序且确认成功条数
     * @param data
     */
    private void syncSend(byte[] data) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, loopGetPartitionId(), null, data);
        try{
            RecordMetadata recordMetadata = producer.send(record).get();
            MockDataFromFileToKafka.TOTAL.add(1L);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 异步发
     * 不保证顺序且确认成功条数
     * @param data
     */
    private void asyncSend(byte[] data) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, loopGetPartitionId(), null, data);
        producer.send(record, callBack);
    }

    private int loopGetPartitionId() {
        if (currentIndex == partitions.length) {
            currentIndex = 0;
        }
        return partitions[currentIndex++];
    }

    static class ProducerCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception == null){
                MockDataFromFileToKafka.TOTAL.add(1L);
            }else {
                logger.error("发送失败", exception);
            }
        }
    }

}
