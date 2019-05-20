/*
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

package org.apache.kylin.storage;

import org.apache.kylin.metadata.realization.IRealization;

public interface IStorage {

    /**
     * 创建一个查询对象IStorageQuery，用来查询给定的IRealization。简单来说，就是返回一个能够查询指定Cube的对象。
     * IRealization是在Cube之上的一个抽象。其主要的实现就是Cube，此外还有被称为Hybrid的联合Cube。
     *
     * @param realization
     * @return
     */
    public IStorageQuery createQuery(IRealization realization);


    /**
     * 适配指定的构建引擎接口。返回一个对象，实现指定的OUT接口。该接口主要由计算引擎调用，
     * 要求存储引擎向计算引擎适配。如果存储引擎无法提供指定接口的实现，则适配失败，Cube构建无法进行。
     *
     * @param engineInterface
     * @param <I>
     * @return
     */
    public <I> I adaptToBuildEngine(Class<I> engineInterface);
}
