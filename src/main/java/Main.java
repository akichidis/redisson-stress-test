import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Created by tasos on 27/02/2018.
 */
public class Main {
    private static final Logger LOG = LogManager.getLogger(Main.class);
    private static List<UUID> profileIds;
    private static RedisClient redisClient;
    private static ExecutorService executorService;
    private static final Random random = new Random();
    private static final int TOTAL_UUIDS = 200000;

    public static void main(String[] args) {
        generateUUIds();

        setupRedisClient();

        setupExecutors();
    }

    private static void setupExecutors() {
        executorService = Executors.newFixedThreadPool(30);

        IntStream.range(0, 20_000_000).forEach(i -> {
            executorService.execute(() -> {
                if (shouldRead()) {
                    redisClient.getProfile(getRandomProfileId());
                } else {
                    redisClient.addProfile(getRandomProfileId().toString());
                }
            });
        });
    }

    private static UUID getRandomProfileId() {
        return profileIds.get(random.nextInt(TOTAL_UUIDS));
    }

    private static boolean shouldRead() {
        return random.nextInt() >= 0.6;
    }

    private static void generateUUIds() {
        LOG.info("Started generating uuids...");
        profileIds = new ArrayList<>();

        IntStream.range(0, TOTAL_UUIDS).forEach(i -> {
            profileIds.add(UUID.randomUUID());
        });

        LOG.info("Generated the uuids...");
    }

    private static void setupRedisClient() {
        redisClient = new RedisClient();
    }
}
