package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    //
    @Override
    public Result queryHotBlog(Integer current) {
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog->{
            this.isBlogLiked(blog);
            this.queryBlogUser(blog);
        });
        return Result.ok(records);
    }


    // 根据笔记id查询笔记详情
    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if(blog == null){
            return Result.fail("笔记不存在");
        }
        queryBlogUser(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    @Override
    public Result likeBlog(Long id) {
        // 1. 获取当前用户id
        Long userId = UserHolder.getUser().getId();
        String key = "blog:like:" + id;
        Double score = stringRedisTemplate.opsForZSet().score(key,userId.toString());
        // 2. 判断用户是否在点赞集合中

        // 2.1 不在点赞集合中，点赞数+1，并将userId添加到集合中
        if(score == null){
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if(isSuccess){
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else{
            // 2.1 在点赞集合中，点赞数-1，并将userId从集合中移除
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if(isSuccess){
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }

        return Result.ok();
    }

    // 查询点赞前5名
    @Override
    public Result queryBlogLikes(Long id) {
        //1.查询top5的点赞用户
        String key=BLOG_LIKED_KEY+id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if(top5==null||top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //2.解析出其中的id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",",ids);
        //3.根据用户id查询用户
        //SELECT *from tb_user where id IN(5,1) ORDER BY FIELD(id,5,1)
        List<UserDTO> userDTOS = userService.query()
                .in("id",ids).last("order by field(id,"+idStr+")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        //4.返回
        return Result.ok(userDTOS);
    }


    // 根据笔记的作者id查询作者的昵称和头像
    private void queryBlogUser(Blog blog){
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
    private void isBlogLiked(Blog blog){
        // 如果用户未登录，无需查询用户是否点赞
        UserDTO user = UserHolder.getUser();
        if(user == null){
            return;
        }
        Long userId = UserHolder.getUser().getId();
        String key = "blog:like:" + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key,userId.toString());
        blog.setIsLike(score != null );
    }
}
