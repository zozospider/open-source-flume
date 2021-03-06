/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

/**
 * Serializer that simply returns the passed in value
 * 只是返回传入值的 Serializer
 */
public class RegexExtractorInterceptorPassThroughSerializer implements
    RegexExtractorInterceptorSerializer {

  @Override
  public String serialize(String value) {
    return value;
  }

  @Override
  public void configure(Context context) {
    // NO-OP...
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    // NO-OP...
  }

}
