package cn.edu.tsinghua.iginx.filesystem.file.property;

// 给出时序列，转换为文件系统的路径
public final class FilePath {
    private static String SEPARATOR = System.getProperty("file.separator");
    private static String MYSEPARATOR = "/";
    private String oriSeries;
    private String fileName;
    public static String MYWILDCARD = "#";
    public static String WILDCARD = "*";
    private static String FILEPATHFORMAT = "%s%s" + MYSEPARATOR + "%s.iginx";
    private static String DIRPATHFORMAT = "%s%s" + MYSEPARATOR + "%s" + MYSEPARATOR;

    public FilePath(String oriSeries) {
        this.oriSeries = oriSeries;
        this.fileName = getFileNameFormSeries(oriSeries);
    }

    public static String toIginxPath(String root, String storageUnit, String series) {
        if (series != null) series = series.replace("*", MYWILDCARD);
        if (storageUnit != null) storageUnit = storageUnit.replace("*", MYWILDCARD);
        if (series == null && storageUnit == null || storageUnit.equals(MYWILDCARD)) {
            return root;
        }
        // 之后根据规则修改获取文件名的方法， may fix it
        if (series == null && storageUnit != null) {
            return String.format(DIRPATHFORMAT, root, storageUnit, "");
        }
        if (series.equals(MYWILDCARD)) {
            return String.format(FILEPATHFORMAT, root, storageUnit, MYWILDCARD);
        }
        String middlePath = series.substring(0, series.lastIndexOf("."));
        return String.format(
                FILEPATHFORMAT,
                root,
                storageUnit,
                middlePath.replace(".", MYSEPARATOR) + MYSEPARATOR + getFileNameFormSeries(series));
    }

    public static String toNormalFilePath(String root, String series) {
        if (series != null) series = series.replace(WILDCARD, MYWILDCARD);
        return root + series.replace(".", MYSEPARATOR);
    }

    public static String getFileNameFormSeries(String series) {
        return series.substring(series.lastIndexOf(".") + 1);
    }

    public static boolean ifDir(String oriSeries) {
        return !oriSeries.contains("##");
    }

    public static String convertFilePathToSeries(String filePath, String separator) {
        return separator + filePath.replace(separator, ".");
    }

    public static String convertAbsolutePathToSeries(
            String root, String filePath, String fileName, String storageUnit) {
        String tmp;
        if (filePath.contains("\\")) {
            filePath = filePath.replaceAll("\\\\", MYSEPARATOR);
        }
        if (storageUnit != null) {
            if (storageUnit.equals(MYWILDCARD) || storageUnit.equals(WILDCARD)) {
                tmp = filePath.substring(filePath.indexOf(root) + root.length() + 1);
                tmp = tmp.substring(tmp.indexOf(MYSEPARATOR) + 1);
            } else {
                tmp =
                        filePath.substring(
                                filePath.indexOf(root) + root.length() + storageUnit.length() + 1);
            }
        } else tmp = filePath.substring(filePath.indexOf(root) + root.length());
        if (tmp.isEmpty()) return SEPARATOR;
        if (tmp.contains(".iginx")) {
            tmp = tmp.substring(0, tmp.lastIndexOf(".iginx"));
        }
        return tmp.replaceAll(MYSEPARATOR, ".");
    }

    public String getOriSeries() {
        return oriSeries;
    }

    public String getFileName() {
        return fileName;
    }

    public static void setSeparator(String SEPARATOR) {
        FilePath.SEPARATOR = SEPARATOR;
    }

    public static String getSEPARATOR() {
        return SEPARATOR;
    }
}
