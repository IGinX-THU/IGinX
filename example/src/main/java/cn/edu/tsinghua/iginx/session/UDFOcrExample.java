package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** 通过UDF，使用OCR对图像进行文字识别 */
public class UDFOcrExample {

  private static Session session;

  public static void main(String[] args) throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    // 打开 Session
    session.openSession();

    // 首先将图像文件夹作为文件系统数据库加入IGinX
    Map<String, String> params = new HashMap<>();
    params.put("iginx_port", "6888");
    params.put("dummy_dir", "example/src/main/resources/OCR/files");
    params.put("is_read_only", "true");
    params.put("has_data", "true");
    params.put("schema_prefix", "OCR");
    session.addStorageEngine("127.0.0.1", 6668, StorageEngineType.filesystem, params);
    try {
      // 查询图片文件，由于.是IGinX的关键字，这里将source.png转写为source\png，接下来我们将用OCR识别source.png
      SessionExecuteSqlResult result = session.executeSql("SHOW COLUMNS;");
      result.print(false, "ms");
      /**
       * Columns:<br>
       * +--------------------+--------+<br>
       * |                Path|DataType|<br>
       * +--------------------+--------+<br>
       * |OCR.files.source\png|  BINARY|<br>
       * +--------------------+--------+<br>
       * Total line number = 1
       */

      // 注册UDF
      session.executeRegisterTask(
          "create function udsf \"ocrtext\" from \"UDSFOcr\" in \"example/src/main/resources/udsf_ocr.py\";");

      try {
        // 调用UDF，这里因为UDF需要返回两列，所以需要使用通配符*以及子查询
        result = session.executeSql("select ocrtext(*) from (select source\\png from OCR.files);");
        result.print(false, "ms");
      } finally {
        session.executeSql("drop function \"ocrtext\";");
      }
    } finally {
      session.removeStorageEngine(
          Collections.singletonList(new RemovedStorageEngineInfo("127.0.0.1", 6668, "OCR", "")));
    }

    session.closeSession();
  }
}
