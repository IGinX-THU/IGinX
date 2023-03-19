package cn.edu.tsinghua.iginx.integration.tool;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.utils.FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ConfLoder {
    private final void logInfo(String info, Object... args) {
        if (DEBUG) logger.info(info, args);
    }
    private static final Logger logger = LoggerFactory.getLogger(ConfLoder.class);
    private static List<String> storageEngines = new ArrayList<>();
    private List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();
    private Map<DBConf.DBType, List<String>> taskMap = new HashMap<>();
    private static String confPath;
    private boolean DEBUG = false;
    private static String STORAGEENGINELIST = "storageEngineList";
    private String TESTTASK = "test-list";
    private static String DBCONF = "%s-config";
    private String RUNNINGSTORAGE = "./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt";

    public String getStorageType() {
        String storageType = FileReader.convertToString(RUNNINGSTORAGE);
        logInfo("run the test on {}", storageType);
        return storageType;
    }

    public ConfLoder(String confPath) {
        this.confPath = confPath;
    }

    public void loadTestConf() throws IOException {
        InputStream in = new FileInputStream(confPath);
        Properties properties = new Properties();
        properties.load(in);
        logInfo("loading the test conf...", null);
        String property = properties.getProperty(STORAGEENGINELIST);
        if (property == null || property.length() == 0) return;
        storageEngines.addAll(Arrays.asList(property.split(",")));

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
            storageEngineMetas.add(new StorageEngineMeta(-1, ip, port, extraParams, storageEngine, -1));
        }

        // load the task list
        for (String storageEngine : storageEngines) {
            String tasks = null;
            if (storageEngine.equalsIgnoreCase("influxdb")) {
                tasks = properties.getProperty("influxdb-" + TESTTASK);
            } else {
                tasks = properties.getProperty(TESTTASK);
            }
            logInfo("the task of {} is :", storageEngine);
            List<String> oriTaskList = Arrays.asList(tasks.split(",")), taskList = new ArrayList<>();
            for(String taskName : oriTaskList) {
                if (taskName.contains("{}")) {
                    taskName = taskName.replace("{}", storageEngine);
                }
                taskList.add(taskName);
                logInfo("{}", taskName);
            }
            taskMap.put(DBConf.getDBType(storageEngine), taskList);
        }
    }

    public DBConf loadDBConf() throws IOException {
        InputStream in = Files.newInputStream(Paths.get(confPath));
        Properties properties = new Properties();
        properties.load(in);
        logInfo("loading the DB conf...");
        String property = properties.getProperty(STORAGEENGINELIST);
        if (property == null || property.length() == 0) return null;
        storageEngines.addAll(Arrays.asList(property.split(",")));

        DBConf dbConf = new DBConf();
        // load the DB conf
        for (String storageEngine : storageEngines) {
            String confs = properties.getProperty(String.format(DBCONF, storageEngine));
            logInfo("the task of {} is : {}", storageEngine, confs);
            List<String> confList = Arrays.asList(confs.split(","));
            for(String conf : confList) {
                String[] confKV = conf.split("=");
                dbConf.setEnumValue(DBConf.getDBConfType(confKV[0]), Boolean.parseBoolean(confKV[1]));
            }
        }
        return dbConf;
    }



    public Map<DBConf.DBType, List<String>> getTaskMap() {
        return taskMap;
    }

    public List<String> getStorageEngines() {
        return storageEngines;
    }

    public List<StorageEngineMeta> getStorageEngineMetas() {
        return storageEngineMetas;
    }
}
