/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.mssqlclient.impl.protocol.datatype;

// NUMERIC, NUMERICN, DECIMAL, or DECIMALN.
public class NumericDataType extends MSSQLDataType {
  private final int precision;
  private final int scale;

  public NumericDataType(int id, Class<?> mappedJavaType, int precision, int scale) {
    super(id, mappedJavaType);
    this.precision = precision;
    this.scale = scale;
  }

  public int precision() {
    return precision;
  }

  public int scale() {
    return scale;
  }
}
