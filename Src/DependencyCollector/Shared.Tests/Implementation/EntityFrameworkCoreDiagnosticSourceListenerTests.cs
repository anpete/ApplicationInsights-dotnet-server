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
    using System.Data.Common;

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

            var commandExecutedEventData = new
            {
                CommandId = commandId,
                ExecuteMethod = DbCommandMethod.ExecuteReader,
                ConnectionId = sqlConnection.ClientConnectionId,
                Command = sqlCommand,
                IsAsync = true,
                StartTime = DateTimeOffset.UtcNow,
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
        
        [TestMethod]
        public void TracksCommandError()
        {
            var commandId = Guid.NewGuid();
            var sqlConnection = new SqlConnection(TestConnectionString);
            var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = "select * from orders";

            var commandErrorEventData = new
            {
                CommandId = commandId,
                ExecuteMethod = DbCommandMethod.ExecuteReader,
                ConnectionId = sqlConnection.ClientConnectionId,
                Command = sqlCommand,
                Exception = new Exception("Boom!"),
                StartTime = DateTimeOffset.UtcNow
            };

            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
                EntityFrameworkCoreDiagnosticSourceListener.CommandErrorEventId,
                commandErrorEventData);

            var exceptionTelemetry = (ExceptionTelemetry)this.sendItems.Single();

            Assert.AreSame(commandErrorEventData.Exception, exceptionTelemetry.Exception);
            Assert.AreEqual(commandErrorEventData.Exception.Message, exceptionTelemetry.Message);
            Assert.AreEqual(commandErrorEventData.StartTime, exceptionTelemetry.Timestamp);
        }

        [TestMethod]
        public void TracksConnectionOpened()
        {
            var sqlConnection = new SqlConnection(TestConnectionString);

            var connectionEndEventData = new
            {
                ConnectionId = sqlConnection.ClientConnectionId,
                Connection = sqlConnection,
                IsAsync = true,
                StartTime = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromMilliseconds(50)
            };

            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
                EntityFrameworkCoreDiagnosticSourceListener.ConnectionOpenedEventId,
                connectionEndEventData);

            var dependencyTelemetry = (DependencyTelemetry)this.sendItems.Single();

            Assert.AreEqual(connectionEndEventData.ConnectionId.ToString("N"), dependencyTelemetry.Id);
            Assert.AreEqual("Open", dependencyTelemetry.Data);
            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master | Open", dependencyTelemetry.Name);
            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Target);
            Assert.AreEqual(RemoteDependencyConstants.SQL, dependencyTelemetry.Type);
            Assert.IsTrue((bool)dependencyTelemetry.Success);
            Assert.AreEqual(connectionEndEventData.StartTime, dependencyTelemetry.Timestamp);
            Assert.AreEqual(connectionEndEventData.Duration, dependencyTelemetry.Duration);
        }

        [TestMethod]
        public void TracksConnectionClosed()
        {
            var operationId = Guid.NewGuid();
            var sqlConnection = new SqlConnection(TestConnectionString);
            
            var connectionEndEventData = new
            {
                ConnectionId = sqlConnection.ClientConnectionId,
                Connection = sqlConnection,
                IsAsync = true,
                StartTime = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromMilliseconds(50)
            };

            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
                EntityFrameworkCoreDiagnosticSourceListener.ConnectionClosedEventId,
                connectionEndEventData);

            var dependencyTelemetry = (DependencyTelemetry)this.sendItems.Single();

            Assert.AreEqual(connectionEndEventData.ConnectionId.ToString("N"), dependencyTelemetry.Id);
            Assert.AreEqual("Close", dependencyTelemetry.Data);
            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master | Close", dependencyTelemetry.Name);
            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Target);
            Assert.AreEqual(RemoteDependencyConstants.SQL, dependencyTelemetry.Type);
            Assert.IsTrue((bool)dependencyTelemetry.Success);
            Assert.AreEqual(connectionEndEventData.StartTime, dependencyTelemetry.Timestamp);
            Assert.AreEqual(connectionEndEventData.Duration, dependencyTelemetry.Duration);
        }

        [TestMethod]
        public void TracksConnectionError()
        {
            var sqlConnection = new SqlConnection(TestConnectionString);
            
            var errorOpenEventData = new
            {
                ConnectionId = sqlConnection.ClientConnectionId,
                Connection = sqlConnection,
                Exception = new Exception("Boom!"),
                StartTime = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromMilliseconds(50)
            };

            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
                EntityFrameworkCoreDiagnosticSourceListener.ConnectionErrorEventId,
                errorOpenEventData);

            var exceptionTelemetry = (ExceptionTelemetry)this.sendItems.Single();

            Assert.AreSame(errorOpenEventData.Exception, exceptionTelemetry.Exception);
            Assert.AreEqual(errorOpenEventData.Exception.Message, exceptionTelemetry.Message);
            Assert.AreEqual(errorOpenEventData.StartTime, exceptionTelemetry.Timestamp);
        }

        private class FakeTransaction : DbTransaction
        {
            private DbConnection connection;

            public FakeTransaction(DbConnection connection)
            {
                this.connection = connection;
            }

            public override IsolationLevel IsolationLevel => default(IsolationLevel);

            protected override DbConnection DbConnection => connection;

            public override void Commit()
            {
                throw new NotImplementedException();
            }

            public override void Rollback()
            {
                throw new NotImplementedException();
            }
        }

        [TestMethod]
        public void TracksTransactionCommitted()
        {
            var transactionId = Guid.NewGuid();
            var sqlConnection = new SqlConnection(TestConnectionString);

            var transactionEndEventData = new
            {
                TransactionId = transactionId,
                Transaction = new FakeTransaction(sqlConnection),
                StartTime = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromMilliseconds(50)
            };

            var now = DateTimeOffset.UtcNow;

            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
                EntityFrameworkCoreDiagnosticSourceListener.TransactionCommittedEventId,
                transactionEndEventData);

            var dependencyTelemetry = (DependencyTelemetry)this.sendItems.Single();

            Assert.AreEqual(transactionEndEventData.TransactionId.ToString("N"), dependencyTelemetry.Id);
            Assert.AreEqual("Commit", dependencyTelemetry.Data);
            Assert.AreEqual(
                "(localdb)\\MSSQLLocalDB | master | Commit | " + transactionEndEventData.Transaction.IsolationLevel, 
                dependencyTelemetry.Name);
            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Target);
            Assert.AreEqual(RemoteDependencyConstants.SQL, dependencyTelemetry.Type);
            Assert.IsTrue((bool)dependencyTelemetry.Success);
            Assert.AreEqual(transactionEndEventData.StartTime, dependencyTelemetry.Timestamp);
            Assert.AreEqual(transactionEndEventData.Duration, dependencyTelemetry.Duration);
        }

        [TestMethod]
        public void TracksTransactionRolledBack()
        {
            var transactionId = Guid.NewGuid();
            var sqlConnection = new SqlConnection(TestConnectionString);

            var transactionEndEventData = new
            {
                TransactionId = transactionId,
                Transaction = new FakeTransaction(sqlConnection),
                StartTime = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromMilliseconds(50)
            };

            var now = DateTimeOffset.UtcNow;

            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
                EntityFrameworkCoreDiagnosticSourceListener.TransactionRolledBackEventId,
                transactionEndEventData);

            var dependencyTelemetry = (DependencyTelemetry)this.sendItems.Single();

            Assert.AreEqual(transactionEndEventData.TransactionId.ToString("N"), dependencyTelemetry.Id);
            Assert.AreEqual("Rollback", dependencyTelemetry.Data);
            Assert.AreEqual(
                "(localdb)\\MSSQLLocalDB | master | Rollback | " + transactionEndEventData.Transaction.IsolationLevel, 
                dependencyTelemetry.Name);
            Assert.AreEqual("(localdb)\\MSSQLLocalDB | master", dependencyTelemetry.Target);
            Assert.AreEqual(RemoteDependencyConstants.SQL, dependencyTelemetry.Type);
            Assert.IsTrue((bool)dependencyTelemetry.Success);
            Assert.AreEqual(transactionEndEventData.StartTime, dependencyTelemetry.Timestamp);
            Assert.AreEqual(transactionEndEventData.Duration, dependencyTelemetry.Duration);
        }

        [TestMethod]
        public void TracksTransactionError()
        {
            var transactionId = Guid.NewGuid();
            var sqlConnection = new SqlConnection(TestConnectionString);

            var transactionErrorEventData = new
            {
                TransactionId = transactionId,
                Exception = new Exception("Boom!"),
                StartTime = DateTimeOffset.UtcNow
            };

            var now = DateTimeOffset.UtcNow;

            this.fakeEntityFrameworkCoreClientDiagnosticSource.Write(
                EntityFrameworkCoreDiagnosticSourceListener.TransactionErrorEventId,
                transactionErrorEventData);

            var exceptionTelemetry = (ExceptionTelemetry)this.sendItems.Single();

            Assert.AreSame(transactionErrorEventData.Exception, exceptionTelemetry.Exception);
            Assert.AreEqual(transactionErrorEventData.Exception.Message, exceptionTelemetry.Message);
            Assert.AreEqual(transactionErrorEventData.StartTime, exceptionTelemetry.Timestamp);
        }

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