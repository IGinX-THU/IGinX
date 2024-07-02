/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
