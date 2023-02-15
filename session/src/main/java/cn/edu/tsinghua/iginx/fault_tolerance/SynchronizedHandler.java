package cn.edu.tsinghua.iginx.fault_tolerance;

import cn.edu.tsinghua.iginx.thrift.IService;
import org.apache.thrift.TException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SynchronizedHandler implements InvocationHandler {

    private final IService.Iface client;

    public SynchronizedHandler(IService.Iface client) {
        this.client = client;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            synchronized(this.client) {
                return method.invoke(this.client, args);
            }
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof TException) {
                throw e.getTargetException();
            } else {
                throw new TException("Error in calling method " + method.getName(), e.getTargetException());
            }
        } catch (Exception e) {
            throw new TException("Error in calling method " + method.getName(), e);
        }
    }

}
