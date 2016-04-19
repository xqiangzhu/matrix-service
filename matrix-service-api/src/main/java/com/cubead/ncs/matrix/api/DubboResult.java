package com.cubead.ncs.matrix.api;

import java.io.Serializable;

/**
 * dubbo 调用结果
 * 
 * @author kangye
 * @param <T>
 */
public class DubboResult<T> implements Serializable {

    private static final long serialVersionUID = 425283513536725914L;

    private ResultStatus resultStatus;

    private String message;

    private T bean;

    public enum ResultStatus {
        SUCCESS, FAIL
    }

    public ResultStatus getResultStatus() {
        return resultStatus;
    }

    public void setResultStatus(ResultStatus resultStatus) {
        this.resultStatus = resultStatus;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getBean() {
        return bean;
    }

    public void setBean(T bean) {
        this.bean = bean;
    }

    public void setMessageAndStatus(String message, ResultStatus resultStatus) {
        this.message = message;
        this.resultStatus = resultStatus;
    }

    @Override
    public String toString() {
        return "DubboResult [resultStatus=" + resultStatus + ", message=" + message + ", bean=" + bean + "]";
    }
}
