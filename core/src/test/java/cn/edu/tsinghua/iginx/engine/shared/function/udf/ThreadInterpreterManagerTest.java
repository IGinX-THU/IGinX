/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.shared.function.udf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDF;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonException;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

/**
 * Runs Pemjax (>= 0.2.4) timeout regressions through the real embedded CPython/JNI boundary.
 *
 * <p>Python-only edge cases live in {@code udf/pemjax_timeout_regression.py}; the tests here drive
 * the same code paths IGinX uses in production ({@link ThreadInterpreterManager} and {@link
 * PyUDF}).
 */
public class ThreadInterpreterManagerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadInterpreterManagerTest.class);

  private static final Config CONFIG = ConfigDescriptor.getInstance().getConfig();

  private static final String PYTHON_CMD =
      System.getProperty("pemjax.python", CONFIG.getPythonCMD());

  private static final String TERMINATED = "; terminated.";

  private static final String PENDING = "cancellation pending";

  private static PythonInterpreter interpreter;
  private static int sequence;

  /** Exposes the protected production entry point used by UDTF/UDAF/UDSF. */
  private static final class RegressionPyUDF extends PyUDF {
    RegressionPyUDF() {
      super("pemjax_timeout_regression", "BlockingUDF");
    }

    List<List<Object>> call(List<List<Object>> data) {
      return invokePyUDF(data, new ArrayList<>(), new HashMap<>());
    }

    @Override
    public FunctionType getFunctionType() {
      return FunctionType.UDF;
    }

    @Override
    public MappingType getMappingType() {
      return MappingType.Mapping;
    }

    @Override
    public String getIdentifier() {
      return "pemjax_timeout_regression_udf";
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    String resources =
        Paths.get(ThreadInterpreterManagerTest.class.getResource("/udf").toURI()).toString();
    ThreadInterpreterManager.setConfig(
        PythonInterpreterConfig.newBuilder()
            .setPythonExec(PYTHON_CMD)
            .addPythonPaths(resources)
            .build());
    interpreter = ThreadInterpreterManager.getInterpreter();
    interpreter.exec("import pemjax_timeout_regression as regression");
  }

  @After
  public void cleanPendingTasks() {
    interpreter.exec("regression.cleanup()");
  }

  @AfterClass
  public static void tearDown() {
    if (interpreter != null) {
      // Keep the interpreter alive until every worker and hook has exited.
      interpreter.exec("regression.cleanup()");
      interpreter.close();
    }
  }

  private String newProbe() {
    String name = "timeout_probe_" + sequence++;
    interpreter.exec(name + " = regression.Probe()");
    return name;
  }

  private static List<List<Object>> invoke(long timeout, String object, String method) {
    return ThreadInterpreterManager.invokeMethodWithTimeout(
        timeout, object, method, new ArrayList<>(), new ArrayList<>(), new HashMap<>());
  }

  private static void assertTimeout(Exception error, boolean terminated) {
    assertTrue(error.toString(), error instanceof PythonException);
    String message = String.valueOf(error.getMessage());
    assertTrue(message, message.contains("timeout"));
    assertTrue(message, message.contains(terminated ? TERMINATED : PENDING));
  }

  private void expectManagerTimeout(String object, String method) {
    expectManagerTimeout(object, method, true);
  }

  private void expectManagerTimeout(String object, String method, boolean terminated) {
    try {
      invoke(1, object, method);
      fail("Expected a Python timeout");
    } catch (Exception expected) {
      assertTimeout(expected, terminated);
    }
  }

  @Test
  public void normalResultSurvivesTheJniBoundary() {
    assertEquals(Arrays.asList(Arrays.asList(1L, 2L, 3L)), invoke(5, newProbe(), "normal"));
  }

  @Test
  public void disabledTimeoutCallsTheUdfDirectly() {
    assertEquals(Arrays.asList(Arrays.asList(1L, 2L, 3L)), invoke(0, newProbe(), "normal"));
  }

  @Test
  public void udfErrorWithinTheDeadlineIsNotReportedAsTimeout() {
    try {
      invoke(5, newProbe(), "fail_row");
      fail("Expected the UDF error");
    } catch (Exception expected) {
      assertTrue(expected.toString(), expected instanceof PythonException);
      String message = String.valueOf(expected.getMessage());
      assertTrue(message, message.contains("bad row in pemjax regression"));
      assertFalse(message, message.contains("Thread timeout"));
    }
  }

  @Test
  public void checkpointStopsPythonExecutionAndFurtherSideEffects() {
    String object = newProbe();
    expectManagerTimeout(object, "spin");
    interpreter.exec("regression.verify_stopped(" + object + ")");
  }

  @Test
  public void publicPackageCheckpointCancelsTheLegacyJniWrapper() {
    String object = "public_checkpoint_probe_" + sequence++;
    interpreter.exec(object + " = regression.PublicCheckpointProbe()");
    expectManagerTimeout(object, "spin");
    interpreter.exec("regression.verify_stopped(" + object + ")");
  }

  @Test
  public void udfExceptionHandlerCannotTurnTimeoutIntoSuccess() {
    String object = newProbe();
    expectManagerTimeout(object, "catch_exception");
    interpreter.exec("regression.verify_stopped(" + object + ")");
  }

  @Test
  public void checkpointOutsideBareExceptStopsATimedOutUdf() {
    // Broad per-row handlers must not surround the cancellation checkpoint.
    String object = newProbe();
    expectManagerTimeout(object, "swallow_per_row");
    interpreter.exec("regression.verify_stopped(" + object + ")");
  }

  @Test
  public void resultProducedAfterTheDeadlineIsStillATimeout() {
    expectManagerTimeout(newProbe(), "slow_return");
  }

  @Test
  public void timedOutPyUdfDoesNotBlockTheNextQueryNorWriteLate() {
    // PyUDF re-instantiates the UDF object on every call, so a stuck worker of an earlier call
    // must be tracked without blocking the next query, and must not act once released.
    long previous = CONFIG.getUDFTimeout();
    CONFIG.setUDFTimeout(1);
    RegressionPyUDF udf = new RegressionPyUDF();
    List<List<Object>> rows =
        Arrays.asList(Collections.<Object>singletonList(1), Collections.<Object>singletonList(2));
    try {
      interpreter.exec("regression.prepare_udf_block()");
      try {
        udf.call(rows);
        fail("Expected a Python timeout");
      } catch (Exception expected) {
        assertTimeout(expected, false);
      }
      assertEquals(true, interpreter.invoke("regression.verify_udf_pending"));
      assertEquals(Arrays.asList(Collections.singletonList(2L)), udf.call(rows));
      assertEquals(true, interpreter.invoke("regression.verify_udf_released"));
    } finally {
      CONFIG.setUDFTimeout(previous);
    }
  }

  @Test
  public void timeoutDuringImportDoesNotPoisonLaterImports() {
    String object = newProbe();
    interpreter.exec("regression.prepare_import()");
    expectManagerTimeout(object, "importing", false);
    interpreter.exec("regression.verify_import(" + object + ")");
    assertEquals(2L, interpreter.invoke("regression.child_success", 1));
  }

  @Test
  public void pythonIdentifierAndNullRollbackWorkInEmbeddedInterpreter() {
    assertEquals(true, interpreter.invoke("regression.check_ident_and_rollback"));
  }

  @Test
  public void blockedWorkerIsTrackedAndItsObjectCannotBeReused() {
    assertEquals(true, interpreter.invoke("regression.check_pending_and_reuse"));
  }

  @Test
  public void blockingInterruptHookCannotBlockTheWatchdog() {
    assertEquals(true, interpreter.invoke("regression.check_blocking_interrupt"));
  }

  @Test
  public void interruptHookCanReleaseANativeWait() {
    assertEquals(true, interpreter.invoke("regression.check_interrupt_releases_wait"));
  }

  @Test
  public void catchingBaseExceptionStillReportsTimeout() {
    assertEquals(true, interpreter.invoke("regression.check_swallowed_cancellation"));
  }

  @Test
  public void checkpointsOutsideNestedAndCallerHandlersStop() {
    assertEquals(true, interpreter.invoke("regression.check_swallow_variants"));
  }

  @Test
  public void wrappedGeneratorHasTimeoutForEachIteration() {
    assertEquals(true, interpreter.invoke("regression.check_generator"));
  }

  @Test
  public void generatorReturnedByRegularFunctionIsTimed() {
    assertEquals(true, interpreter.invoke("regression.check_generator_from_function"));
  }

  @Test
  public void resultsAndExceptionsPropagateWithoutTimeout() {
    assertEquals(true, interpreter.invoke("regression.check_exceptions_propagate"));
  }

  @Test
  public void invalidTimeoutsAndReuseOfATaskAreRejected() {
    assertEquals(true, interpreter.invoke("regression.check_invalid_timeouts"));
  }

  @Test
  public void oneTimedOutCallDoesNotDisturbConcurrentCalls() {
    assertEquals(true, interpreter.invoke("regression.check_concurrent_calls"));
  }

  @Test
  public void isolatedProcessIsKilledAndSubsequentCallsStillWork() {
    assertEquals(true, interpreter.invoke("regression.check_process", PYTHON_CMD));
  }

  @Test
  public void processTimeoutKillsDescendantsHoldingItsPipes() {
    assertEquals(
        true, interpreter.invoke("regression.check_process_kills_descendants", PYTHON_CMD));
  }

  @Test
  public void processModeWrapperDecoratorAndErrorsWork() {
    assertEquals(true, interpreter.invoke("regression.check_process_modes", PYTHON_CMD));
  }

  @Test
  public void timeoutDoesNotInterruptNormalOrExceptionalCleanup() {
    assertEquals(true, interpreter.invoke("regression.check_cleanup_is_not_interrupted"));
  }

  @Test
  public void uncooperativeThreadIsReportedPendingUntilItExits() {
    assertEquals(
        true, interpreter.invoke("regression.check_uncooperative_work_is_honestly_pending"));
  }

  @Test
  public void defaultProcessExecutableFailsFastOrWorksUnderJni() {
    // Under JNI sys.executable may name the JVM; callers must pass python_executable.
    LOGGER.info("{}", interpreter.invoke("regression.check_default_executable"));
  }
}
