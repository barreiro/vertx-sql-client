/*
 * Copyright (C) 2017 Julien Viet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.vertx.sqlclient.impl.pool;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.PromiseInternal;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.ConnectionFactory;
import io.vertx.sqlclient.impl.command.CommandBase;
import io.vertx.sqlclient.spi.DatabaseMetadata;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Alternative implementation of ConnectionPool inspired by Agroal's pool
 *
 * @author <a href="mailto:lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalConnectionPool {

  private final ConnectionFactory connector;
  private final ContextInternal context;
  private final int maxSize;
  private final int maxWaitQueueSize;
  private final ArrayDeque<Handler<AsyncResult<Connection>>> waiters = new ArrayDeque<>();
  private final List<PooledConnection> allConnections = new StampedCopyOnWriteArrayList<>(PooledConnection.class);
  private final LongAdder creation = new LongAdder();
  private ThreadLocal<List<PooledConnection>> localCache; // TODO: clear this from time to time

  private boolean checkInProgress;
  private boolean closed;

  public AgroalConnectionPool(ConnectionFactory connector, int maxSize) {
    this(connector, maxSize, PoolOptions.DEFAULT_MAX_WAIT_QUEUE_SIZE);
  }

  public AgroalConnectionPool(ConnectionFactory connector, int maxSize, int maxWaitQueueSize) {
    this(connector, null, maxSize, maxWaitQueueSize);
  }

  public AgroalConnectionPool(ConnectionFactory connector, Context context, int maxSize, int maxWaitQueueSize) {
    Objects.requireNonNull(connector, "No null connector");
    if (maxSize < 1) {
      throw new IllegalArgumentException("Pool max size must be > 0");
    }
    this.localCache = new PooledConnectionThreadLocal();
    this.maxSize = maxSize;
    this.context = (ContextInternal) context;
    this.maxWaitQueueSize = maxWaitQueueSize;
    this.connector = connector;
  }

  private static final class PooledConnectionThreadLocal extends ThreadLocal<List<PooledConnection>> {

    @Override
    protected List<PooledConnection> initialValue() {
      return new UncheckedArrayList<>( PooledConnection.class );
    }
  }

  public int available() {
    int available = 0;
    for (PooledConnection connection : allConnections) {
      if (connection.isAvailable()) {
        available++;
      }
    }
    return available;
  }

  public int size() {
    return allConnections.size();
  }

  public void acquire(Handler<AsyncResult<Connection>> waiter) {
    if (context != null) {
      context.dispatch(waiter, this::doAcquire);
    } else {
      doAcquire(waiter);
    }
  }

  private void doAcquire(Handler<AsyncResult<Connection>> waiter) {
    if (closed) {
      IllegalStateException err = new IllegalStateException("Connection pool closed");
      if (context != null) {
        waiter.handle(context.failedFuture(err));
      } else {
        waiter.handle(Future.failedFuture(err));
      }
      return;
    }
    waiters.add(waiter);
    check();
  }

  public Future<Void> close() {
    PromiseInternal<Void> promise = context.promise();
    context.dispatch(promise, this::close);
    return promise.future();
  }

  public void close(Promise<Void> promise) {
    if (closed) {
      promise.fail("Connection pool already closed");
      return;
    }
    closed = true;
    Future<Connection> failure = Future.failedFuture("Connection pool closed");
    for (Handler<AsyncResult<Connection>> pending : waiters) {
      try {
        pending.handle(failure);
      } catch (Exception ignore) {
      }
    }
    List<Future> futures = new ArrayList<>();
    for (PooledConnection pooled : allConnections) {
      Promise<Void> p = Promise.promise();
      pooled.close(p);
      futures.add(p.future());
    }
    CompositeFuture
      .join(futures)
      .<Void>mapEmpty()
      .onComplete(promise);
  }

  private static class PooledConnection implements Connection, Connection.Holder  {

    private static final AtomicReferenceFieldUpdater<PooledConnection, State> stateUpdater = newUpdater( PooledConnection.class, State.class, "state" );

    private final Connection conn;
    private final AgroalConnectionPool pool;
    private Holder holder;
    private volatile State state;

    PooledConnection(Connection conn, AgroalConnectionPool pool) {
      this.conn = conn;
      this.pool = pool;
      this.state = State.IN_USE;
    }

    @Override
    public boolean isSsl() {
      return conn.isSsl();
    }

    public boolean isAvailable() {
      return stateUpdater.get(this).equals(State.AVAILABLE);
    }

    public boolean take() {
      return stateUpdater.compareAndSet(this, State.AVAILABLE, State.IN_USE);
    }

    @Override
    public DatabaseMetadata getDatabaseMetaData() {
      return conn.getDatabaseMetaData();
    }

    @Override
    public <R> void schedule(CommandBase<R> cmd, Promise<R> handler) {
      conn.schedule(cmd, handler);
    }

    /**
     * Close the underlying connection
     */
    private void close(Promise<Void> promise) {
      stateUpdater.set(this, State.UNAVAILABLE);
      conn.close(this, promise);
    }

    @Override
    public void init(Holder holder) {
      if (this.holder != null) {
        throw new IllegalStateException();
      }
      this.holder = holder;
    }

    @Override
    public void close(Holder holder, Promise<Void> promise) {
      if (pool.context != null) {
        pool.context.dispatch(v -> doClose(holder, promise));
      } else {
        doClose(holder, promise);
      }
    }

    private void doClose(Holder holder, Promise<Void> promise) {
      if (holder != this.holder) {
        String msg;
        if (this.holder == null) {
          msg = "Connection released twice";
        } else {
          msg = "Connection released by " + holder + " owned by " + this.holder;
        }
        // Log it ?
        promise.fail(msg);
        return;
      }
      this.holder = null;
      stateUpdater.set(this, State.AVAILABLE);
      pool.localCache.get().add(this);
      pool.check();
      promise.complete();
    }

    @Override
    public void handleClosed() {
      if (pool.allConnections.remove(this)) {
        if (holder == null) {
          stateUpdater.set(this, State.AVAILABLE);
        } else {
          holder.handleClosed();
        }
        pool.check();
      } else {
        throw new IllegalStateException();
      }
    }

    @Override
    public void handleEvent(Object event) {
      if (holder != null) {
        holder.handleEvent(event);
      }
    }

    @Override
    public void handleException(Throwable err) {
      if (holder != null) {
        holder.handleException(err);
      }
    }

    @Override
    public int getProcessId() {
      return conn.getProcessId();
    }

    @Override
    public int getSecretKey() {
      return conn.getSecretKey();
    }

    private enum State {
      AVAILABLE, IN_USE, UNAVAILABLE
    }
  }

  private void check() {
    if (closed) {
      return;
    }
    if (!checkInProgress) {
      checkInProgress = true;
      try {
        Handler<AsyncResult<Connection>> waiter;
        while ((waiter = waiters.poll()) != null) {

          // thread-local cache
          List<PooledConnection> cached = localCache.get();
          while (!cached.isEmpty()) {
            PooledConnection proxy = cached.remove(cached.size() - 1);
            if (proxy.take()) {
              waiter.handle(Future.succeededFuture(proxy));
              return;
            }
          }

          // go through all connections
          for (PooledConnection proxy : allConnections) {
            if (proxy.take()) {
              waiter.handle(Future.succeededFuture(proxy));
              return;
            }
          }

          // no avail, attempt to get a slot to create new
          if (creation.intValue() + allConnections.size() < maxSize) {
            creation.increment();
            Handler<AsyncResult<Connection>> finalWaiter = waiter;
            connector.connect().onComplete(ar -> {
              creation.decrement();
              if (ar.succeeded()) {
                Connection conn = ar.result();
                PooledConnection proxy = new PooledConnection(conn, this);
                allConnections.add(proxy);
                conn.init(proxy);
                finalWaiter.handle(Future.succeededFuture(proxy));
              } else {
                finalWaiter.handle(Future.failedFuture(ar.cause()));
                check();
              }
            });
          } else {
            // we wait
            waiters.add(waiter);

            // may need to reduce the queue if maxWaitQueueSize was exceeded
            if (maxWaitQueueSize >= 0) {
              while (waiters.size() > maxWaitQueueSize) {
                waiters.pollLast().handle(Future.failedFuture("Max waiter size reached"));
              }
            }
            break;
          }
        }
      } finally {
        checkInProgress = false;
      }
    }
  }
}
