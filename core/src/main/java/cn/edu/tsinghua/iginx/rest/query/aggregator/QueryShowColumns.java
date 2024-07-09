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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryShowColumns extends QueryAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryShowColumns.class);

  public QueryShowColumns() {
    super(QueryAggregatorType.SHOW_COLUMNS);
  }

  public QueryResultDataset doAggregate(RestSession session) {
    QueryResultDataset queryResultDataset = new QueryResultDataset();
    try {
      SessionQueryDataSet sessionQueryDataSet = session.showColumns();
      queryResultDataset.setPaths(getPathsFromShowColumns(sessionQueryDataSet));
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
    return queryResultDataset;
  }
}
