package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopServiceImpl;
    @Resource
    private CacheClient cacheClient;

    @Test
    public void test(){
        Shop shop = shopServiceImpl.getById(1L);
//        cacheClient.(CACHE_SHOP_KEY+1L,shop,10L, TimeUnit.SECONDS);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY+1L,shop,10L,TimeUnit.MINUTES);
    }

}
