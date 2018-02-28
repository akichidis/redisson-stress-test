import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.UUID;

/**
 * Created by tasos on 27/02/2018.
 */
public class RedisClient {
    private static final Logger LOG = LogManager.getLogger(RedisClient.class);
    private final RedissonClient redisClient;

    public RedisClient() {
        Config config = new Config();
        config.useReplicatedServers()
                .addNodeAddress("redis://test-cluster.naw9b6.ng.0001.use1.cache.amazonaws.com:6379")
                .setTimeout(1000);

        redisClient = Redisson.create(config);

        redisClient.getMapCache("profiles").setMaxSize(500000);
    }

    public void addProfile(String profile) {
        try {
            redisClient.getMapCache("profiles").fastPut(UUID.randomUUID().toString(), profile);
            LOG.info("Successfully added profile");
        } catch (Exception ex) {
            LOG.error("Couldn't add profile ", ex);
        }
    }

    public String getProfile(UUID profileId) {
        try {
            RMapCache<String, String> profiles = redisClient.getMapCache("profiles");

            LOG.info("Successfully retrieved profile");

            return profiles.get(profileId.toString());
        } catch (Exception ex) {
            LOG.error("Couldn't get profile ", ex);

            return "";
        }
    }
}
