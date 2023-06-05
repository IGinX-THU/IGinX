package cn.edu.tsinghua.iginx.integration.tool;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.utils.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfLoader {

    private static final Logger logger = LoggerFactory.getLogger(ConfLoader.class);

    private static final String STORAGE_ENGINE_LIST = "storageEngineList";

    private static final String TEST_LIST = "test-list";

    private static final String DBCONF = "%s-config";

    private static final String RUNNING_STORAGE = "./src/test/resources/DBConf.txt";

    private static final String IS_SCALING = "./src/test/resources/isScaling.txt";

    private static List<String> storageEngines = new ArrayList<>();

    private List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();

    private Map<DBType, List<String>> taskMap = new HashMap<>();

    private static String confPath;

    private boolean DEBUG = false;

    private void logInfo(String info, Object... args) {
        if (DEBUG) {
            logger.info(info, args);
        }
    }

    public String getStorageType() {
        String storageType = FileReader.convertToString(RUNNING_STORAGE);
        logInfo("run the test on {}", storageType);
        return storageType;
    }

    public boolean isScaling() {
        String isScaling = FileReader.convertToString(this.IS_SCALING);
        logInfo("{}", isScaling);
        return isScaling != null && !isScaling.isEmpty();
    }

    public ConfLoader(String confPath) {
        this.confPath = confPath;
    }

    public void loadTestConf() throws IOException {
        InputStream in = Files.newInputStream(Paths.get(confPath));
        Properties properties = new Properties();
        properties.load(in);
        logInfo("loading the test conf...");
        String property = properties.getProperty(STORAGE_ENGINE_LIST);
        if (property == null || property.isEmpty()) {
            return;
        }
        storageEngines = Arrays.asList(property.split(","));

        // load the storageEngine
        for (String storageEngine : storageEngines) {
            String storageEngineInfo = properties.getProperty(storageEngine);
            logInfo("load the info of {} : {}", storageEngine, storageEngineInfo);
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
            storageEngineMetas.add(
                    new StorageEngineMeta(-1, ip, port, extraParams, storageEngine, -1));
        }

        // load the task list
        for (String storageEngine : storageEngines) {
            String tasks;
            String storage = storageEngine.toLowerCase();
            tasks = properties.getProperty(storage + "-" + TEST_LIST);
            if (tasks == null) {
                tasks = properties.getProperty(TEST_LIST);
            }
            logInfo("the task of {} is :", storageEngine);
            List<String> oriTaskList = Arrays.asList(tasks.split(",")),
                    taskList = new ArrayList<>();
            for (String taskName : oriTaskList) {
                if (taskName.contains("{}")) {
                    taskName = taskName.replace("{}", storageEngine);
                }
                taskList.add(taskName);
                logInfo("{}", taskName);
            }
            taskMap.put(DBType.valueOf(storageEngine.toLowerCase()), taskList);
        }
    }

    public DBConf loadDBConf(String storageEngine) {
        DBConf dbConf = new DBConf();
        Properties properties;
        try {
            InputStream in = Files.newInputStream(Paths.get(confPath));
            properties = new Properties();
            properties.load(in);
        } catch (IOException e) {
            logger.error("load conf failure: {}", e.getMessage());
            return dbConf;
        }

        logInfo("loading the DB conf...");
        String property = properties.getProperty(STORAGE_ENGINE_LIST);
        if (property == null || property.isEmpty()) {
            return dbConf;
        }
        storageEngines = Arrays.asList(property.split(","));

        if (storageEngine == null || storageEngine.isEmpty()) {
            return dbConf;
        }
        String confs = properties.getProperty(String.format(DBCONF, storageEngine));
        logInfo("the conf of {} is : {}", storageEngine, confs);
        String[] confList = confs.split(",");
        for (String conf : confList) {
            String[] confKV = conf.split("=");
            dbConf.setEnumValue(DBConf.getDBConfType(confKV[0]), Boolean.parseBoolean(confKV[1]));
        }
        return dbConf;
    }

    public Map<DBType, List<String>> getTaskMap() {
        return taskMap;
    }

    public List<StorageEngineMeta> getStorageEngineMetas() {
        return storageEngineMetas;
    }
}
