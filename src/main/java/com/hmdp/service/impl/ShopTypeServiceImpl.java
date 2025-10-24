package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IShopTypeService typeService;
    @Override
    public Result queryCacheList() {
        String key = "cache:" + "list";
        List<String> cacheList =  stringRedisTemplate.opsForList().range(key,0 , -1);
        if (cacheList != null && !cacheList.isEmpty()) {
            // 3.1 缓存命中，将JSON字符串转换为对象并返回
            List<ShopType> typeList = new ArrayList<>();
            for (String jsonStr : cacheList) {
                ShopType type = JSONUtil.toBean(jsonStr, ShopType.class);
                typeList.add(type);
            }
            return Result.ok(typeList);
        }
        List<ShopType> typeList = typeService
                .query().orderByAsc("sort").list();

        // redis和数据库中都不存在
        if(typeList.isEmpty()){
            return Result.fail("列表信息不存在");
        }
        // 数据库中存在，redis中不存在，更新redis
        try {
            // 6.1 将对象转换为JSON字符串
            List<String> jsonListToCache = new ArrayList<>();
            for (ShopType type : typeList) {
                jsonListToCache.add(JSONUtil.toJsonStr(type));
            }
            // 6.2 批量写入Redis列表（使用右插，保持顺序）
            stringRedisTemplate.opsForList().rightPushAll(key, jsonListToCache);
            // 6.3 为缓存设置过期时间，例如12小时，避免数据长期不更新
            stringRedisTemplate.expire(key, 12, TimeUnit.HOURS);

        } catch (Exception e) {
            // 记录日志，缓存写入失败不应影响主流程
             log.error("写入商品类型列表到缓存失败: ", e);
        }
        //将从数据库中查询到的列表添加到redis中
        return Result.ok(typeList);
    }
}
