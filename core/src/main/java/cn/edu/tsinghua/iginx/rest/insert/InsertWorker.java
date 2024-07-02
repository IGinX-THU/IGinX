package cn.edu.tsinghua.iginx.rest.insert;

import cn.edu.tsinghua.iginx.rest.bean.Query;
import cn.edu.tsinghua.iginx.rest.bean.QueryResult;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

public class InsertWorker extends Thread {
  private static final String NO_CACHE = "no-cache";
  private final HttpHeaders httpheaders;
  private InputStream stream;
  private QueryResult preQueryResult;
  private Query preQuery;
  private final AsyncResponse asyncResponse;
  private final boolean isAnnotation;
  private final boolean isAppend;

  public InsertWorker(
      final AsyncResponse asyncResponse,
      HttpHeaders httpheaders,
      InputStream stream,
      boolean isAnnotation) {
    this.asyncResponse = asyncResponse;
    this.httpheaders = httpheaders;
    this.stream = stream;
    this.isAnnotation = isAnnotation;
    this.isAppend = false;
  }

  public InsertWorker(
      final AsyncResponse asyncResponse,
      HttpHeaders httpheaders,
      QueryResult preQueryResult,
      Query preQuery,
      boolean isAppend) {
    this.asyncResponse = asyncResponse;
    this.httpheaders = httpheaders;
    this.preQueryResult = preQueryResult;
    this.preQuery = preQuery;
    this.isAnnotation = true;
    this.isAppend = isAppend;
  }

  static Response.ResponseBuilder setHeaders(Response.ResponseBuilder responseBuilder) {
    responseBuilder.header("Access-Control-Allow-Origin", "*");
    responseBuilder.header("Pragma", NO_CACHE);
    responseBuilder.header("Cache-Control", NO_CACHE);
    responseBuilder.header("Expires", 0);
    return responseBuilder;
  }

  @Override
  public void run() {
    Response response;
    try {
      if (httpheaders != null) {
        List<String> requestHeader = httpheaders.getRequestHeader("Content-Encoding");
        if (requestHeader != null && requestHeader.contains("gzip")) {
          stream = new GZIPInputStream(stream);
        }
      }
      if (!isAppend && !isAnnotation) {
        DataPointsParser parser =
            new DataPointsParser(new InputStreamReader(stream, StandardCharsets.UTF_8));
        parser.parse();
      } else if (isAppend) {
        DataPointsParser parser = new DataPointsParser();
        parser.handleAnnotationAppend(preQueryResult);
      } else {
        DataPointsParser parser = new DataPointsParser();
        parser.handleAnnotationUpdate(preQuery, preQueryResult);
      }
      response = Response.status(Response.Status.OK).build();
    } catch (Exception e) {
      response =
          setHeaders(Response.status(Response.Status.BAD_REQUEST).entity(e.toString())).build();
    }
    asyncResponse.resume(response);
  }
}
