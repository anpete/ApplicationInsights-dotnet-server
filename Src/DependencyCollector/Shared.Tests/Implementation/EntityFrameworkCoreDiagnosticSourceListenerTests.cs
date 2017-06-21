using System.Data.SqlClient;
using System.Linq;
using Microsoft.ApplicationInsights.DataContracts;

namespace Microsoft.ApplicationInsights
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Microsoft.ApplicationInsights.Channel;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.ApplicationInsights.DependencyCollector.Implementation;
    using Microsoft.ApplicationInsights.DependencyCollector;
    using System.Data;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

    [TestClass]
    public class EntityFrameworkCoreDiagnosticSourceListenerTests
    {
        private const string TestConnectionString = "Data Source=(localdb)\\MSSQLLocalDB;Database=master";
        
        private IList<ITelemetry> sendItems;
        private StubTelemetryChannel stubTelemetryChannel;
        private TelemetryConfiguration configuration;
        private FakeEntityFrameworkCoreDiagnosticSource fakeEntityFrameworkCoreClientDiagnosticSource;
        private EntityFrameworkCoreDiagnosticSourceListener entityFrameworkCoreDiagnosticSourceListener;

        [TestInitialize]
        public void TestInit()
        {
            this.sendItems = new List<ITelemetry>();
            this.stubTelemetryChannel = new StubTelemetryChannel {OnSend = item => this.sendItems.Add(item)};

            this.configuration = new TelemetryConfiguration
            {
                InstrumentationKey = Guid.NewGuid().ToString(),
                TelemetryChannel = stubTelemetryChannel
            };

            this.fakeEntityFrameworkCoreClientDiagnosticSource = new FakeEntityFrameworkCoreDiagnosticSource();
            this.entityFrameworkCoreDiagnosticSourceListener = new EntityFrameworkCoreDiagnosticSourceListener(configuration);
        }

        [TestCleanup]
        public void Cleanup()
        {
            this.entityFrameworkCoreDiagnosticSourceListener.Dispose();
            this.fakeEntityFrameworkCoreClientDiagnosticSource.Dispose();
            this.configuration.Dispose();
            this.stubTelemetryChannel.Dispose();
        }

        [TestMethod]
        public void TracksCommandExecuted()
        {
            var commandId = Guid.NewGuid();
            var sqlConnection = new SqlConnection(TestConnectionString);
            var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = "select * from orders";
            
            var now = DateTimeOffset.UtcNow;
            
            var commandExecutedEventData = new
            {
                CommandId = commandId,
                ExecuteMethod = DbCommandMethod.ExecuteReader,
                ConnectionId = sqlConnection.ClientConnectionId,
                Command = sqlCommand,
                IsAsync = true,
                StartTime = now,
                Duration = TimeSpan.FromMilliseconds(50)
            };

            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
                EntityFrameworkCoreDiagnosticSourceListener.CommandExecutedEventId,
                commandExecutedEventData);

            var dependencyTelemetry = (DependencyTelemetry)this.sendItems.Single();

            Assert.AreEqual(commandExecutedEventData.CommandId.ToString("N"), dependencyTelemetry.Id);
            Assert.AreEqual(sqlCommand.CommandText, dependencyTelemetry.Data);
            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Name);
            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Target);
            Assert.AreEqual(RemoteDependencyConstants.SQL, dependencyTelemetry.Type);
            Assert.IsTrue((bool)dependencyTelemetry.Success);
            Assert.AreEqual(commandExecutedEventData.StartTime, dependencyTelemetry.Timestamp);
            Assert.AreEqual(commandExecutedEventData.Duration, dependencyTelemetry.Duration);
        }
        
//        [TestMethod]
//        public void TracksCommandError()
//        {
//            var operationId = Guid.NewGuid();
//            var sqlConnection = new SqlConnection(TestConnectionString);
//            var sqlCommand = sqlConnection.CreateCommand();
//            sqlCommand.CommandText = "select * from orders";
//
//            var beforeExecuteEventData = new
//            {
//                OperationId = operationId,
//                Timestamp = 1000000L
//            };
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlBeforeExecuteCommand,
//                beforeExecuteEventData);
//
//            var commandErrorEventData = new
//            {
//                OperationId = operationId,
//                Operation = "ExecuteReader",
//                ConnectionId = sqlConnection.ClientConnectionId,
//                Command = sqlCommand,
//                Exception = new Exception("Boom!"),
//                Timestamp = 2000000L
//            };
//
//            var now = DateTimeOffset.UtcNow;
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlErrorExecuteCommand,
//                commandErrorEventData);
//
//            var exceptionTelemetry = (ExceptionTelemetry)this.sendItems.Single();
//
//            Assert.AreSame(commandErrorEventData.Exception, exceptionTelemetry.Exception);
//            Assert.AreEqual(commandErrorEventData.Exception.Message, exceptionTelemetry.Message);
//            Assert.IsTrue(exceptionTelemetry.Timestamp.Ticks < now.Ticks);
//        }
//
//        [TestMethod]
//        public void TracksConnectionOpened()
//        {
//            var operationId = Guid.NewGuid();
//            var sqlConnection = new SqlConnection(TestConnectionString);
//
//            var beforeOpenEventData = new
//            {
//                OperationId = operationId,
//                Timestamp = 1000000L
//            };
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlBeforeOpenConnection,
//                beforeOpenEventData);
//
//            var afterOpenEventData = new
//            {
//                OperationId = operationId,
//                Operation = "Open",
//                ConnectionId = sqlConnection.ClientConnectionId,
//                Connection = sqlConnection,
//                Statistics = sqlConnection.RetrieveStatistics(),
//                Timestamp = 2000000L
//            };
//
//            var now = DateTimeOffset.UtcNow;
//            
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlAfterOpenConnection,
//                afterOpenEventData);
//
//            var dependencyTelemetry = (DependencyTelemetry)this.sendItems.Single();
//
//            Assert.AreEqual(afterOpenEventData.OperationId.ToString(), dependencyTelemetry.Id);
//            Assert.AreEqual(afterOpenEventData.Operation, dependencyTelemetry.Data);
//            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master | " + afterOpenEventData.Operation, dependencyTelemetry.Name);
//            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Target);
//            Assert.AreEqual(RemoteDependencyConstants.SQL, dependencyTelemetry.Type);
//            Assert.IsTrue((bool)dependencyTelemetry.Success);
//            Assert.AreEqual(1000000L, dependencyTelemetry.Duration.Ticks);
//            Assert.IsTrue(dependencyTelemetry.Timestamp.Ticks < now.Ticks);
//        }
//        
//        [TestMethod]
//        public void TracksConnectionOpenedError()
//        {
//            var operationId = Guid.NewGuid();
//            var sqlConnection = new SqlConnection(TestConnectionString);
//            
//            var beforeOpenEventData = new
//            {
//                OperationId = operationId,
//                Timestamp = 1000000L
//            };
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlBeforeOpenConnection,
//                beforeOpenEventData);
//
//            var errorOpenEventData = new
//            {
//                OperationId = operationId,
//                Operation = "Open",
//                ConnectionId = sqlConnection.ClientConnectionId,
//                Connection = sqlConnection,
//                Exception = new Exception("Boom!"),
//                Timestamp = 2000000L
//            };
//
//            var now = DateTimeOffset.UtcNow;
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlErrorOpenConnection,
//                errorOpenEventData);
//
//            var exceptionTelemetry = (ExceptionTelemetry)this.sendItems.Single();
//
//            Assert.AreSame(errorOpenEventData.Exception, exceptionTelemetry.Exception);
//            Assert.AreEqual(errorOpenEventData.Exception.Message, exceptionTelemetry.Message);
//            Assert.IsTrue(exceptionTelemetry.Timestamp.Ticks < now.Ticks);
//        }
//
//        [TestMethod]
//        public void TracksConnectionClosed()
//        {
//            var operationId = Guid.NewGuid();
//            var sqlConnection = new SqlConnection(TestConnectionString);
//
//            var beforeCloseEventData = new
//            {
//                OperationId = operationId,
//                Timestamp = 1000000L
//            };
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlBeforeCloseConnection,
//                beforeCloseEventData);
//
//            var afterCloseEventData = new
//            {
//                OperationId = operationId,
//                Operation = "Close",
//                ConnectionId = sqlConnection.ClientConnectionId,
//                Connection = sqlConnection,
//                Statistics = sqlConnection.RetrieveStatistics(),
//                Timestamp = 2000000L
//            };
//
//            var now = DateTimeOffset.UtcNow;
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlAfterCloseConnection,
//                afterCloseEventData);
//
//            var dependencyTelemetry = (DependencyTelemetry)this.sendItems.Single();
//
//            Assert.AreEqual(afterCloseEventData.OperationId.ToString(), dependencyTelemetry.Id);
//            Assert.AreEqual(afterCloseEventData.Operation, dependencyTelemetry.Data);
//            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master | " + afterCloseEventData.Operation, dependencyTelemetry.Name);
//            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Target);
//            Assert.AreEqual(RemoteDependencyConstants.SQL, dependencyTelemetry.Type);
//            Assert.IsTrue((bool)dependencyTelemetry.Success);
//            Assert.AreEqual(1000000L, dependencyTelemetry.Duration.Ticks);
//            Assert.IsTrue(dependencyTelemetry.Timestamp.Ticks < now.Ticks);
//        }
//
//        [TestMethod]
//        public void TracksConnectionCloseError()
//        {
//            var operationId = Guid.NewGuid();
//            var sqlConnection = new SqlConnection(TestConnectionString);
//
//            var beforeOpenEventData = new
//            {
//                OperationId = operationId,
//                Timestamp = 1000000L
//            };
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlBeforeCloseConnection,
//                beforeOpenEventData);
//
//            var errorOpenEventData = new
//            {
//                OperationId = operationId,
//                Operation = "Close",
//                ConnectionId = sqlConnection.ClientConnectionId,
//                Connection = sqlConnection,
//                Exception = new Exception("Boom!"),
//                Timestamp = 2000000L
//            };
//
//            var now = DateTimeOffset.UtcNow;
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlErrorCloseConnection,
//                errorOpenEventData);
//
//            var exceptionTelemetry = (ExceptionTelemetry)this.sendItems.Single();
//
//            Assert.AreSame(errorOpenEventData.Exception, exceptionTelemetry.Exception);
//            Assert.AreEqual(errorOpenEventData.Exception.Message, exceptionTelemetry.Message);
//            Assert.IsTrue(exceptionTelemetry.Timestamp.Ticks < now.Ticks);
//        }
//
//        [TestMethod]
//        public void TracksTransactionCommitted()
//        {
//            var operationId = Guid.NewGuid();
//            var sqlConnection = new SqlConnection(TestConnectionString);
//
//            var beforeCommitEventData = new
//            {
//                OperationId = operationId,
//                Timestamp = 1000000L
//            };
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlBeforeCommitTransaction,
//                beforeCommitEventData);
//
//            var afterCommitEventData = new
//            {
//                OperationId = operationId,
//                Operation = "Commit",
//                IsolationLevel = IsolationLevel.Snapshot,
//                Connection = sqlConnection,
//                Timestamp = 2000000L
//            };
//
//            var now = DateTimeOffset.UtcNow;
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlAfterCommitTransaction,
//                afterCommitEventData);
//
//            var dependencyTelemetry = (DependencyTelemetry)this.sendItems.Single();
//
//            Assert.AreEqual(afterCommitEventData.OperationId.ToString(), dependencyTelemetry.Id);
//            Assert.AreEqual(afterCommitEventData.Operation, dependencyTelemetry.Data);
//            Assert.AreEqual(
//                "(localdb)\\MSSQLLocalDB | master | " + afterCommitEventData.Operation + " | " + afterCommitEventData.IsolationLevel, 
//                dependencyTelemetry.Name);
//            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Target);
//            Assert.AreEqual(RemoteDependencyConstants.SQL, dependencyTelemetry.Type);
//            Assert.IsTrue((bool)dependencyTelemetry.Success);
//            Assert.AreEqual(1000000L, dependencyTelemetry.Duration.Ticks);
//            Assert.IsTrue(dependencyTelemetry.Timestamp.Ticks < now.Ticks);
//        }
//
//        [TestMethod]
//        public void TracksTransactionCommitError()
//        {
//            var operationId = Guid.NewGuid();
//            var sqlConnection = new SqlConnection(TestConnectionString);
//
//            var beforeCommitEventData = new
//            {
//                OperationId = operationId,
//                Timestamp = 1000000L
//            };
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlBeforeCommitTransaction,
//                beforeCommitEventData);
//
//            var errorCommitEventData = new
//            {
//                OperationId = operationId,
//                Operation = "Commit",
//                IsolationLevel = IsolationLevel.Snapshot,
//                Connection = sqlConnection,
//                Exception = new Exception("Boom!"),
//                Timestamp = 2000000L
//            };
//
//            var now = DateTimeOffset.UtcNow;
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlErrorCommitTransaction,
//                errorCommitEventData);
//
//            var exceptionTelemetry = (ExceptionTelemetry)this.sendItems.Single();
//
//            Assert.AreSame(errorCommitEventData.Exception, exceptionTelemetry.Exception);
//            Assert.AreEqual(errorCommitEventData.Exception.Message, exceptionTelemetry.Message);
//            Assert.IsTrue(exceptionTelemetry.Timestamp.Ticks < now.Ticks);
//        }
//
//        [TestMethod]
//        public void TracksTransactionRolledBack()
//        {
//            var operationId = Guid.NewGuid();
//            var sqlConnection = new SqlConnection(TestConnectionString);
//
//            var beforeRollbackEventData = new
//            {
//                OperationId = operationId,
//                Timestamp = 1000000L
//            };
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlBeforeRollbackTransaction,
//                beforeRollbackEventData);
//
//            var afterRollbackEventData = new
//            {
//                OperationId = operationId,
//                Operation = "Rollback",
//                IsolationLevel = IsolationLevel.Snapshot,
//                Connection = sqlConnection,
//                Timestamp = 2000000L
//            };
//
//            var now = DateTimeOffset.UtcNow;
//
//            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
//                EntityFrameworkCoreDiagnosticSourceListener.SqlAfterRollbackTransaction,
//                afterRollbackEventData);
//
//            var dependencyTelemetry = (DependencyTelemetry)this.sendItems.Single();
//
//            Assert.AreEqual(afterRollbackEventData.OperationId.ToString(), dependencyTelemetry.Id);
//            Assert.AreEqual(afterRollbackEventData.Operation, dependencyTelemetry.Data);
//            Assert.AreEqual(
//                "(localdb)\\MSSQLLocalDB | master | " + afterRollbackEventData.Operation + " | " + afterRollbackEventData.IsolationLevel,
//                dependencyTelemetry.Name);
//            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Target);
//            Assert.AreEqual(RemoteDependencyConstants.SQL, dependencyTelemetry.Type);
//            Assert.IsTrue((bool)dependencyTelemetry.Success);
//            Assert.AreEqual(1000000L, dependencyTelemetry.Duration.Ticks);
//            Assert.IsTrue(dependencyTelemetry.Timestamp.Ticks < now.Ticks);
//        }

        private enum DbCommandMethod
        {
            ExecuteNonQuery,
            ExecuteScalar,
            ExecuteReader
        }
        
        private class FakeEntityFrameworkCoreDiagnosticSource : IDisposable
        {
            private readonly DiagnosticListener listener;

            public FakeEntityFrameworkCoreDiagnosticSource()
            {
                this.listener = new DiagnosticListener(EntityFrameworkCoreDiagnosticSourceListener.DiagnosticListenerName);
            }

            public void Write(string name, object value)
            {
                this.listener.Write(name, value);
            }

            public void Dispose()
            {
                this.listener.Dispose();
            }
        }
    }

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}