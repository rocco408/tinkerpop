﻿﻿#region License

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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    public class SideEffectTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact]
        public void ShouldReturnCachedSideEffectWhenGetIsCalledAfterClose()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);
            var t = g.V().Aggregate("a").Iterate();

            t.SideEffects.Get("a");
            t.SideEffects.Close();
            var results = t.SideEffects.Get("a");

            Assert.NotNull(results);
        }

        [Fact]
        public void ShouldThrowWhenGetIsCalledAfterCloseAndNoSideEffectsAreCachec()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);
            var t = g.V().Aggregate("a").Iterate();

            t.SideEffects.Close();
            Assert.Throws<InvalidOperationException>(() => t.SideEffects.Get("a"));
        }

        [Fact]
        public void ShouldThrowWhenGetIsCalledAfterDisposeAndNoSideEffectsAreCachec()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);
            var t = g.V().Aggregate("a").Iterate();

            t.SideEffects.Dispose();
            Assert.Throws<InvalidOperationException>(() => t.SideEffects.Get("a"));
        }

        [Fact]
        public void ShouldThrowWhenGetIsCalledWithAnUnknownKey()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);
            var t = g.V().Iterate();

            Assert.Throws<KeyNotFoundException>(() => t.SideEffects.Get("m"));
        }

        [Fact]
        public void ShouldReturnAnEmptyCollectionWhenKeysIsCalledForTraversalWithoutSideEffect()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var t = g.V().Iterate();
            var keys = t.SideEffects.Keys();

            Assert.Equal(0, keys.Count);
        }

        [Fact]
        public void ShouldReturnCachedKeysWhenForCloseAfterSomeGet()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);
            var t = g.V().Aggregate("a").Aggregate("b").Iterate();

            t.SideEffects.Get("a");
            t.SideEffects.Close();
            var keys = t.SideEffects.Keys();

            Assert.Equal(2, keys.Count);
            Assert.Contains("a", keys);
            Assert.Contains("b", keys);
        }

        [Fact]
        public void ShouldReturnSideEffectKeyWhenKeysIsCalledForNamedGroupCount()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);
            var t = g.V().Out("created").GroupCount("m").By("name").Iterate();

            var keys = t.SideEffects.Keys();

            var keysList = keys.ToList();
            Assert.Equal(1, keysList.Count);
            Assert.Contains("m", keysList);
        }

        [Fact]
        public async Task ShouldReturnSideEffectsKeysWhenKeysIsCalledOnTraversalThatExecutedAsynchronously()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var t = await g.V().Aggregate("a").Promise(x => x);
            var keys = t.SideEffects.Keys();

            Assert.Equal(1, keys.Count);
            Assert.Contains("a", keys);
        }

        [Fact]
        public async Task ShouldReturnSideEffectValueWhenGetIsCalledOnTraversalThatExecutedAsynchronously()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var t = await g.V().Aggregate("a").Promise(x => x);
            var value = t.SideEffects.Get("a");

            Assert.NotNull(value);
        }

        [Fact]
        public async Task ShouldNotThrowWhenCloseIsCalledOnTraversalThatExecutedAsynchronously()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var t = await g.V().Aggregate("a").Promise(x => x);
            t.SideEffects.Close();
        }
    }
}