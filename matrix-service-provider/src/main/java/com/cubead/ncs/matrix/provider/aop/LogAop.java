package com.cubead.ncs.matrix.provider.aop;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 自定义log拦截器
 * 
 * @author kangye
 */
@Component
@Aspect
public class LogAop {

    private static Logger logger = LoggerFactory.getLogger(LogAop.class);

    @Pointcut("execution(* com.cubead.ncs.matrix.provider.out..*.*(..))")
    public void pointCut() {
    }

    @Before("pointCut()")
    public void before(JoinPoint point) {
        // 获取类名
        String className = point.getTarget().getClass().getName();
        // 获取方法名
        String methodName = point.getSignature().getName();
        // 参数列表
        Object[] params = point.getArgs();
        String strparams = "";
        for (Object param : params) {
            strparams += " " + (param == null ? "null" : param.toString());
        }
        logger.debug(" \n --- input --> className: " + className + ", methodName:" + methodName + ", params:"
                + strparams);
    }

    /*
     * @After("pointCut()") public void after(JoinPoint point) { logger.debug(
     * "after aspect executed"); }
     */

    @AfterReturning(pointcut = "pointCut()", returning = "returnVal")
    public void afterReturning(JoinPoint point, Object returnVal) {
        // 获取类名
        String className = point.getTarget().getClass().getName();
        // 获取方法名
        String methodName = point.getSignature().getName();
        logger.debug("\n --- output --> className: " + className + ", methodName:" + methodName + ", return:"
                + returnVal);
    }

    /*
     * @Around("pointCut()") public Object around(ProceedingJoinPoint point)
     * throws Throwable { return point.proceed(); }
     * 
     * @AfterThrowing(pointcut = "pointCut()", throwing = "error") public void
     * afterThrowing(JoinPoint jp, Throwable error) { logger.debug("error:" +
     * error); }
     */
}
