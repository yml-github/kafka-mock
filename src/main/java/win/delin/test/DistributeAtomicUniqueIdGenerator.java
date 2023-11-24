package win.delin.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author delin
 * @date 2020/4/14
 */
public class DistributeAtomicUniqueIdGenerator {
    private static final Logger logger = LoggerFactory.getLogger(DistributeAtomicUniqueIdGenerator.class);
    private static final int LONG_HOLD_BIT = 63;
    private static final long Y2K_IN_MILLIS = 946656000000L;
    private static final long Y2K = Y2K_IN_MILLIS >> 10;
    private static final long Y2K_MINUTE = Y2K >> 6;

    interface TimestampGetter {
        long getCurrentTimestamp(long millis);
    }

    static class CurrentTimeMillisGetter implements TimestampGetter {
        @Override
        public long getCurrentTimestamp(long millis) {
            return millis - Y2K_IN_MILLIS;
        }
    }

    static class CurrentTimeSecondGetter implements TimestampGetter {
        @Override
        public long getCurrentTimestamp(long millis) {
            return (millis >> 10) - Y2K;
        }
    }

    static class CurrentTimeMinuteGetter implements TimestampGetter {
        @Override
        public long getCurrentTimestamp(long millis) {
            return (millis >> 16) - Y2K_MINUTE;
        }
    }

    private boolean usingSecond;
    private int timeLeftShitBits;
    private int uniqueIdBits;
    private int uniqueId;
    private int maxSequence;

    private AtomicLong sequence = new AtomicLong();
    private AtomicLong lastCurrentTimestamp = new AtomicLong(0);
    private TimestampGetter timestampGetter;

    private boolean usingReset = false;

    private static class LazyHolder {
        private static final DistributeAtomicUniqueIdGenerator INSTANCE = new DistributeAtomicUniqueIdGenerator();
    }

    public static DistributeAtomicUniqueIdGenerator getInstance() {
        return LazyHolder.INSTANCE;
    }

    public DistributeAtomicUniqueIdGenerator() {
        initial(new Random().nextInt(1 << 8));
    }

    private void initial(int uniqueId, int uniqueIdBits, boolean usingSecond, int timestampBits) {
        this.usingSecond = usingSecond;
        this.timestampGetter = usingSecond ? new CurrentTimeSecondGetter() : new CurrentTimeMinuteGetter();
        this.timeLeftShitBits = LONG_HOLD_BIT - timestampBits;
        this.uniqueIdBits = uniqueIdBits;
        this.maxSequence = (int) (Math.pow(2, this.timeLeftShitBits - uniqueIdBits) - 1);

        int maxUniqueId = (int) (Math.pow(2, this.uniqueIdBits) - 1);
        if (uniqueId >= maxUniqueId) {
            logger.warn("collector id {} exceed max {}, generated eventId cann't grantee unique.", uniqueId, maxUniqueId);
        }
        this.uniqueId = uniqueId % maxUniqueId;

    }

    /**
     * default max unique id range: 2^8 = 256, max speed  2^16 = 65535 Event per millis second
     */
    private void initial(int collectorId) {
//		int collectorId = 0;
        int uniqueIdBits = 8;
        boolean usingSecond = false;
        int timestampBits = 40 - 16;
        initial(collectorId, uniqueIdBits, usingSecond, timestampBits);
        this.usingReset = false;
    }

    public long generateUniqueEventId() {
        long currentTimestamp = timestampGetter.getCurrentTimestamp(System.currentTimeMillis());

        if (usingReset) {
            if (currentTimestamp != lastCurrentTimestamp.get()) {
                lastCurrentTimestamp.set(currentTimestamp);
                sequence.set(0);
            }

            return (currentTimestamp << timeLeftShitBits) | ((sequence.incrementAndGet()) << uniqueIdBits) | uniqueId;
        }

        return (currentTimestamp << timeLeftShitBits) | ((sequence.incrementAndGet() % maxSequence) << uniqueIdBits) | uniqueId;
    }

    public long generateBaseId(long millis) {
        return timestampGetter.getCurrentTimestamp(millis) << timeLeftShitBits;
    }

    public long getMillis(long eventId) {
        long time = eventId >> timeLeftShitBits;
        return usingSecond ? ((time + Y2K) << 10) : ((time + Y2K_MINUTE) << 16);
    }

    public long generateBaseIdOffset(long millis) {
        return usingSecond ? ((millis >> 10) << timeLeftShitBits) : (millis << timeLeftShitBits);
    }

}
