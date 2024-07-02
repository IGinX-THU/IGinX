package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.Query;
import java.util.List;
import java.util.function.Consumer;

public interface QueryClient {

  IginXTable query(final Query query) throws IginXException;

  <M> List<M> query(final Query query, final Class<M> measurementType) throws IginXException;

  IginXTable query(final String query) throws IginXException;

  void query(final Query query, final Consumer<IginXRecord> onNext) throws IginXException;

  void query(final String query, final Consumer<IginXRecord> onNext) throws IginXException;

  <M> List<M> query(final String query, final Class<M> measurementType) throws IginXException;

  <M> void query(final String query, final Class<M> measurementType, final Consumer<M> onNext)
      throws IginXException;

  <M> void query(final Query query, final Class<M> measurementType, final Consumer<M> onNext)
      throws IginXException;
}
