package io.pivotal.pal.tracker.allocations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils;
import org.springframework.web.client.RestOperations;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ProjectClient {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final RestOperations restOperations;
    private final String registrationServerEndpoint;
    private ConcurrentMap<Long,ProjectInfo> cache = new ConcurrentHashMap<Long,ProjectInfo>();
    private RedisConnectionFactory redisConnectionFactory = new JedisConnectionFactory();

    public ProjectClient(RestOperations restOperations, String registrationServerEndpoint) {
        this.restOperations= restOperations;
        this.registrationServerEndpoint = registrationServerEndpoint;
    }

    @HystrixCommand(fallbackMethod = "getProjectFromCache")
    public ProjectInfo getProject(long projectId) {
        ProjectInfo projectToCache = restOperations.getForObject(registrationServerEndpoint + "/projects/" + projectId, ProjectInfo.class);
        cache.put(projectId,projectToCache);
        return projectToCache;
    }

    public ProjectInfo getProjectFromCache(long projectId) {
        logger.info("Getting project with id {} from cache", projectId);
        return cache.get(projectId);
    }

    private void addProjectInfoToRedisCache(long projectId, ProjectInfo projectInfo) {
        RedisConnection connection = RedisConnectionUtils.getConnection(this.redisConnectionFactory);
        try {
            ObjectMapper objMapper = new ObjectMapper();
            String projectInfoToSet = null;
            try {
                projectInfoToSet = objMapper.writeValueAsString(projectInfo);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            ((StringRedisConnection) connection).set(String.valueOf(projectId),projectInfoToSet);
        }
        finally {
            RedisConnectionUtils.releaseConnection(connection, this.redisConnectionFactory);
        }
    }

    protected ProjectInfo getProjectFromRedisCache(long projectId) throws Exception {
        ProjectInfo projectInfoToReturn = null;
        RedisConnection connection = RedisConnectionUtils.getConnection(this.redisConnectionFactory);
        try {
            String projectInfo = ((StringRedisConnection) connection).get(String.valueOf(projectId));
            ObjectMapper objMapper = new ObjectMapper();
            projectInfoToReturn = objMapper.readValue(projectInfo, ProjectInfo.class);
        }
        finally {
            RedisConnectionUtils.releaseConnection(connection, this.redisConnectionFactory);
        }
        return projectInfoToReturn;
    }
}
