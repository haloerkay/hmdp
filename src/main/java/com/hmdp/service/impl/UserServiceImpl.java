package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {


    @Resource
    StringRedisTemplate stringRedisTemplate;

    /**
     * 发送验证码
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误！");
        }
        String code = RandomUtil.randomNumbers(6);
//        session.setAttribute("code",code);
        // 生成验证码并存入redis，有效期为2分钟
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        log.info("验证码发送成功,为{}",code);

        return Result.ok();
    }


    /**
     * 用户登录功能实现
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 注意，此处应该有bug，即获取到验证码后将手机号改为其他人的，然后就可以登录到其他人的账号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误");
        }
        String code = loginForm.getCode();

        String cache_code = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if(!code.equals(cache_code)){
            return Result.fail("验证码错误");
        }
        // 使用MP根据phone查询用户
        User user = query().eq("phone", phone).one();

        if(user == null){
            // 用户不存在，则添加新用户
            user = createUserWithPhone(phone);
        }

        String token = UUID.randomUUID().toString();
        UserDTO userDTO = BeanUtil.copyProperties(user,UserDTO.class);
        Map<String, Object> map = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue) ->fieldValue.toString()));
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,map);
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL, TimeUnit.HOURS);

        log.info("登录成功");
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取今天是这个月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.写入redis setbit key offset 1
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取今天是这个月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.获取本月截止今天为止所有的签到记录，返回的是一个十进制的数字
        List<Long> result = stringRedisTemplate.opsForValue()
                .bitField(key, BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                        .valueAt(0));
        if(result==null||result.isEmpty()){
            //没有任何签到结果
            return Result.ok(0);
        }
        Long num = result.get(0);
        if(num==0||num==null){
            return Result.ok(0);
        }
        //6.循坏遍历
        int count=0;
        while (true) {
            //让这个数字与1做与运算，得到数字的最后一个bit位，判断这个bit是否为0
            if((num&1)==0) {
                //如果为0，未签到
                break;
            }else {
                //如果不为0，已签到，计算器+1
                count++;
                //把数字右移一位，抛弃最后一个bit位，继续下一个bit位
                num>>>=1;
            }
        }
        return Result.ok(count);

    }

    private User createUserWithPhone(String phone){
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        // MP保存用户
        save(user);
        log.info("注册成功");
        return user;
    }
}
