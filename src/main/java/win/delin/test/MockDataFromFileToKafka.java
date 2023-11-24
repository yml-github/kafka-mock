package win.delin.test;

import cn.hutool.core.collection.CollectionUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author delin
 * @date 2019/11/14
 */
public class MockDataFromFileToKafka {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MockDataFromFileToKafka.class);

    public static LongAdder TOTAL = new LongAdder();
    public static long START = System.currentTimeMillis();
    public static void main(String[] args) throws Exception{
        Properties prop = new Properties();
        String userdir = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        prop.load(new FileInputStream(userdir + "/" + "mockData.properties"));

        String brokerListStr = prop.getProperty("brokers");
        String topic = prop.getProperty("topic");
        String filepath = prop.getProperty("sourcePath");
        String sourceType = prop.getProperty("sourceType");
        boolean kerberos = Boolean.parseBoolean(String.valueOf(prop.getOrDefault("kerberos.enable", "false")));

        String days = prop.getProperty("minusDay");
        double permitsPerSecond = Double.parseDouble(prop.getProperty("permitsPerSecond"));
        //启动线程数
        int threadSize = Integer.parseInt(String.valueOf(prop.getOrDefault("threadSize", (Double.valueOf(permitsPerSecond).intValue() / 3000) + 1)));
        //选择发送方式  async  sync  onlySend
        String sendType = prop.getProperty("sendType");
        boolean sourceReload = Boolean.parseBoolean(prop.getProperty("sourceReload"));

        LOGGER.info("==========================");
        LOGGER.info("brokerList: {}",  brokerListStr );
        LOGGER.info("to topic: {}",  topic);
        LOGGER.info("mock sourcePath: {}",  filepath);
        LOGGER.info("mock sourceType: {}",  sourceType);
        LOGGER.info("mock sourceReload: {}",  sourceReload);
        LOGGER.info("minus days: {}",  days);
        LOGGER.info("thread size: {}",  threadSize);
        LOGGER.info("sendType: {}",  sendType);
        LOGGER.info("kerberos: {}",  kerberos);
        LOGGER.info("permitsPerSecond: {}",  permitsPerSecond);
        LOGGER.info("==========================");

        List<String> loadDatas = loadDatas(filepath, sourceType);
        LOGGER.info("总样例数量：{}", loadDatas.size());
        LOGGER.info("==========================");

        int perGroupSize = Math.max(loadDatas.size() / (threadSize + 1), 1);

        double perThreadPermitRate = permitsPerSecond / threadSize;

        List<List<String>> lists = CollectionUtil.split(loadDatas, perGroupSize);
        loadDatas.clear();
        for (int i = 0; i < threadSize; i++) {
            LOGGER.info("Start no.{}" + " producer thread.", i);
            UserKafkaProducer producer;
            if (lists.size() - 1 <= i) {
                producer = new UserKafkaProducer(prop,  lists.get(lists.size() - 1), perThreadPermitRate);
            } else {
                producer = new UserKafkaProducer(prop, lists.get(i), perThreadPermitRate);
            }
            producer.start();
        }

        while (true) {
            Thread.sleep(5000);
            long endTime = System.currentTimeMillis();
            long spent = endTime - START;
            LOGGER.info("Total: {}, Speed: {}eps",  TOTAL.longValue(), String.format("%.2f", TOTAL.longValue()/(spent * 1.0d /1000)));

            if (sourceReload){
                loadDatas(filepath, sourceType);
                LOGGER.info("Reloaded file:"+filepath);
            }
        }
    }

    private static List<String> loadDatas(String filepath, String sourceType) throws IOException {
        LOGGER.info("开始读取样例文件：{}", filepath);
        return FileUtils.readLines(new File(filepath), StandardCharsets.UTF_8);
    }
}
