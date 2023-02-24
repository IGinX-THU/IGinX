package cn.edu.tsinghua.iginx.integration.tool;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.utils.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class TestConfLoder {
    private static final Logger logger = LoggerFactory.getLogger(TestConfLoder.class);
    private List<String> storageEngines = new ArrayList<>();
    private List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();
    private Map<String, List<String>> taskList = new HashMap<>();
    private String confPath;
    private String STORAGEENGINELIST = "storageEngineList";
    private String TESTTASK = "%s-test";

    public TestConfLoder(String confPath) {
        this.confPath = confPath;
    }

    public void loadTestConf() throws IOException {
        InputStream in = new FileInputStream(confPath);
        Properties properties = new Properties();
        properties.load(in);
        logger.info("loading the test conf...");
        String property = properties.getProperty(STORAGEENGINELIST);
        if (property == null || property.length() == 0) return;
        storageEngines.addAll(Arrays.asList(property.split(",")));

        // load the storageEngine
        for (String storageEngine : storageEngines) {
            String storageEngineInfo = properties.getProperty(storageEngine);
            logger.info("load the info of {} : {}", storageEngine, storageEngineInfo);
            String[] storageEngineParts = storageEngineInfo.split("#");
            String ip = storageEngineParts[0];
            int port = -1;
            if (!storageEngineParts[1].equals("")) {
                port = Integer.parseInt(storageEngineParts[1]);
            }
            Map<String, String> extraParams = new HashMap<>();
            String[] KAndV;
            for (int j = 3; j < storageEngineParts.length; j++) {
                if (storageEngineParts[j].contains("\"")) {
                    KAndV = storageEngineParts[j].split("\"");
                    extraParams.put(KAndV[0].substring(0, KAndV[0].length() - 1), KAndV[1]);
                } else {
                    KAndV = storageEngineParts[j].split("=");
                    if (KAndV.length != 2) {
                        logger.error("unexpected storage engine meta info: " + storageEngineInfo);
                        continue;
                    }
                    extraParams.put(KAndV[0], KAndV[1]);
                }
            }
            storageEngineMetas.add(new StorageEngineMeta(-1, ip, port, extraParams, storageEngine, -1));
        }

        // load the task list
        taskList = new HashMap<>();
        for (String storageEngine : storageEngines) {
            String tasks = properties.getProperty(String.format(TESTTASK, storageEngine));
            logger.info("the task of {} is : {}", storageEngine, tasks);
            taskList.put(storageEngine, Arrays.asList(tasks.split(",")));
        }
    }

    public Map<String, List<String>> getTaskList() {
        return taskList;
    }

    public List<String> getStorageEngines() {
        return storageEngines;
    }

    public List<StorageEngineMeta> getStorageEngineMetas() {
        return storageEngineMetas;
    }
}
