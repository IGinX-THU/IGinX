package cn.edu.tsinghua.iginx.filesystem.file.property;

import cn.edu.tsinghua.iginx.filesystem.tools.ConfLoader;

// 给出时序列，转换为文件系统的路径
public final class FilePath {
    private static String SEPARATOR = System.getProperty("file.separator");
    private String oriSeries;
    private String filePath;
    private String fileName;
    private String storageUnit;
    private static String ROOT = ConfLoader.getRootPath();
    public static String MYWILDCARD = "#";
    public static String WILDCARD = "*";
    private static String FILEPATHFORMAT = "%s%s"+SEPARATOR+"%s.iginx";
    private static String DIRPATHFORMAT = "%s%s"+SEPARATOR+"%s"+SEPARATOR;

    public FilePath(String storageUnit, String oriSeries) {
        this.storageUnit= storageUnit;
        this.oriSeries = oriSeries;
        this.fileName = getFileNameFormSeries(oriSeries);
    }

    public static String toIginxPath(String storageUnit, String series) {
        if(series!=null) series = series.replace("*", MYWILDCARD);
        if(storageUnit!=null) storageUnit= storageUnit.replace("*", MYWILDCARD);
        if(series==null && storageUnit==null || storageUnit.equals(MYWILDCARD)){
            return ROOT;
        }
        //之后根据规则修改获取文件名的方法， may fix it
        if(series==null && storageUnit!=null){
            return String.format(DIRPATHFORMAT,ROOT,storageUnit,"");
        }
        if(series.equals(MYWILDCARD)){
            return String.format(FILEPATHFORMAT,ROOT,storageUnit, MYWILDCARD);
        }
        String middlePath=series.substring(0,series.lastIndexOf("."));
        return String.format(FILEPATHFORMAT,ROOT,storageUnit,middlePath.replace(".", SEPARATOR)+SEPARATOR+getFileNameFormSeries(series));
//        filePath = storageUnit == null ? "" : separator + storageUnit + separator + series;
//        filePath = storageUnit == null ? "" : storageUnit + separator + series.replace(".", separator);
    }

    public static String toNormalFilePath(String series) {
        if(series!=null) series = series.replace(WILDCARD, MYWILDCARD);
        return ROOT+series.replace(".",SEPARATOR);
    }

    public static String getFileNameFormSeries(String series) {
        return series.substring(series.lastIndexOf(".")+1);
    }

    public static boolean ifDir(String oriSeries){
        return !oriSeries.contains("##");
    }

    public static String convertFilePathToSeries(String filePath, String separator) {
        return separator + filePath.replace(separator, ".");
    }

    public static String convertAbsolutePathToSeries(String filePath, String fileName, String storageUnit) {
        String tmp;

        if(storageUnit!=null) {
            if(storageUnit.equals(MYWILDCARD) || storageUnit.equals(WILDCARD)) {
                tmp= filePath.substring(filePath.indexOf(ROOT)+ROOT.length()+1);
                tmp= tmp.substring(tmp.indexOf(SEPARATOR)+1);
            } else {
                tmp= filePath.substring(filePath.indexOf(ROOT)+ROOT.length()+storageUnit.length()+1);
            }
        } else
            tmp= filePath.substring(filePath.indexOf(ROOT)+ROOT.length());
        if(tmp.isEmpty()) return SEPARATOR;
        if(tmp.contains(".iginx")) {
            tmp=tmp.substring(0,tmp.lastIndexOf(".iginx"));
        }
        return tmp.replace(SEPARATOR, ".");
    }

    public String getOriSeries() {
        return oriSeries;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public static void setSeparator(String SEPARATOR) {
        FilePath.SEPARATOR = SEPARATOR;
    }
}
