package cn.edu.tsinghua.iginx.vectordb.pool;


import cn.edu.tsinghua.iginx.vectordb.MilvusStorage;
import io.milvus.param.ConnectParam;
import io.milvus.param.RetryParam;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MilvusConnectPoolFactory implements PooledObjectFactory<MilvusClientV2> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MilvusConnectPoolFactory.class);
    private final String username;
    private final String password;
    private final String host;
    private final Integer port;
    private final String protocol;

    public MilvusConnectPoolFactory(String host, Integer port, String protocol,String username, String password) {
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
        this.protocol = protocol;
    }

    /**
     * 每次获取MilvusClient实例时触发
     * @param p
     * @throws Exception
     */
    @Override
    public void activateObject(PooledObject<MilvusClientV2> p) throws Exception {
    }

    /**
     * 注销MilvusClient实例时触发
     * @param p
     * @throws Exception
     */
    @Override
    public void destroyObject(PooledObject<MilvusClientV2> p) throws Exception {
        if (p.getObject()!=null){
            p.getObject().close();
        }
    }

    protected String getUrl(String protocol, String host, int port) {
        return new StringBuilder(protocol)
                .append("://")
                .append(host)
                .append(":")
                .append(port)
                .toString();
    }
    @Override
    public PooledObject<MilvusClientV2> makeObject() throws Exception {
        LOGGER.info("创建MilvusClient实例");
        try {
            ConnectConfig.ConnectConfigBuilder configBuilder = ConnectConfig.builder().uri(getUrl(protocol, host, port));
            if (!StringUtils.isEmpty(username)) {
                configBuilder.username(username);
            }
            if (!StringUtils.isEmpty(password)) {
                configBuilder.password(password);
            }
            ConnectConfig config = configBuilder.build();
            MilvusClientV2 client = new MilvusClientV2(config);
            if (client!=null) {
                return new DefaultPooledObject<>(client);
            }
            throw new RuntimeException("无法创建Milvus数据库连接");
        } catch (Exception e) {
            throw new RuntimeException("无法创建Milvus数据库连接", e);
        }
    }

    /**
     * 归还MilvusClient实例时触发此方法
     * @param p
     * @return
     */
    @Override
    public void passivateObject(PooledObject<MilvusClientV2> p) throws Exception {
    }


    /**
     * 判断MilvusClient实例状态
     * @param p
     * @return
     */
    @Override
    public boolean validateObject(PooledObject<MilvusClientV2> p) {
        if  (p.getObject()!=null){
            return p.getObject().clientIsReady();
        }
        return false;
    }
}
