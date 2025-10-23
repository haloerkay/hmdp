package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

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
        session.setAttribute("code",code);
        log.debug("验证码发送成功,为{}",code);

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
        String session_code = (String) session.getAttribute("code");
        if(!code.equals(session_code)){
            return Result.fail("验证码错误");
        }
        // 使用MP根据phone查询用户
        User user = query().eq("phone", phone).one();

        if(user == null){
            // 用户不存在，则添加新用户
            user = createUserWithPhone(phone);
        }
        // 将user对象中的值赋给userDTO对象，避免敏感信息泄露
        session.setAttribute("user", BeanUtil.copyProperties(user,UserDTO.class));
        log.info("登录成功");
        return Result.ok();
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
