package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private CacheClient cacheClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisIdWorker redisIdWorker;
    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    public void test(){
        Shop shop = shopService.getById(1L);
//        cacheClient.(CACHE_SHOP_KEY+1L,shop,10L, TimeUnit.SECONDS);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY+1L,shop,10L,TimeUnit.MINUTES);
    }


    // 测试全局Id生成器
    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task=()->{
            for (int i = 0; i < 100; i++) {
                Long id = redisIdWorker.nextId("order");
                System.out.println("id = "+id);
            }
            latch.countDown();
        };
        long begin=System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();

        long end=System.currentTimeMillis();
        System.out.println("time："+(end-begin));
    }


    // 导入redisGEO店铺数据
    @Test
    void loadShopDate(){
        //1.查询店铺信息
        List<Shop> list = shopService.list();

        //2.把店铺分组，按照typeId分组，id一致的放到一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //3.分批完成写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //获取类型id
            Long typeId = entry.getKey();
            //获取同类型店铺集合
            List<Shop>  value = entry.getValue();

            String key=SHOP_GEO_KEY+typeId;
            List<RedisGeoCommands.GeoLocation<String>> locations=new ArrayList<>();
            for (Shop shop : value) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(),shop.getY())
                ));
            }
            //写入redis
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

    /**
     * 测试 HyperLogLog 实现 UV 统计的误差
     */
    @Test
    public void testHyperLogLog() {
        String[] values = new String[1000];
        // 批量保存100w条用户记录，每一批1个记录
        int j = 0;
        for (int i = 0; i < 1000000; i++) {
            j = i % 1000;
            values[j] = "user_" + i;
            if (j == 999) {
                // 发送到Redis
                stringRedisTemplate.opsForHyperLogLog().add("hl2", values);
            }
        }
        // 统计数量
        Long count = stringRedisTemplate.opsForHyperLogLog().size("hl2");
        System.out.println("count = " + count);
    }

}
