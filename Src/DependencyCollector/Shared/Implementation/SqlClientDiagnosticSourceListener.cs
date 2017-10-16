namespace Microsoft.ApplicationInsights.DependencyCollector.Implementation
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Globalization;
    using Microsoft.ApplicationInsights.Channel;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.ApplicationInsights.Extensibility.Implementation;
    using Microsoft.ApplicationInsights.Extensibility.Implementation.Tracing;
    using Microsoft.ApplicationInsights.Web.Implementation;
    using Microsoft.ApplicationInsights.DependencyCollector.Implementation.Operation;

    internal class SqlClientDiagnosticSourceListener : IObserver<KeyValuePair<string, object>>, IDisposable
    {
        // Event ids defined at: https://github.com/dotnet/corefx/blob/master/src/System.Data.SqlClient/src/System/Data/SqlClient/SqlClientDiagnosticListenerExtensions.cs
        public const string DiagnosticListenerName = "SqlClientDiagnosticListener";

        public const string SqlBeforeExecuteCommand = SqlClientPrefix + "WriteCommandBefore";
        public const string SqlAfterExecuteCommand = SqlClientPrefix + "WriteCommandAfter";
        public const string SqlErrorExecuteCommand = SqlClientPrefix + "WriteCommandError";
        
        public const string SqlBeforeOpenConnection = SqlClientPrefix + "WriteConnectionOpenBefore";
        public const string SqlAfterOpenConnection = SqlClientPrefix + "WriteConnectionOpenAfter";
        public const string SqlErrorOpenConnection = SqlClientPrefix + "WriteConnectionOpenError";

        public const string SqlBeforeCloseConnection = SqlClientPrefix + "WriteConnectionCloseBefore";
        public const string SqlAfterCloseConnection = SqlClientPrefix + "WriteConnectionCloseAfter";
        public const string SqlErrorCloseConnection = SqlClientPrefix + "WriteConnectionCloseError";

        public const string SqlBeforeCommitTransaction = SqlClientPrefix + "WriteTransactionCommitBefore";
        public const string SqlAfterCommitTransaction = SqlClientPrefix + "WriteTransactionCommitAfter";
        public const string SqlErrorCommitTransaction = SqlClientPrefix + "WriteTransactionCommitError";

        public const string SqlBeforeRollbackTransaction = SqlClientPrefix + "WriteTransactionRollbackBefore";
        public const string SqlAfterRollbackTransaction = SqlClientPrefix + "WriteTransactionRollbackAfter";
        public const string SqlErrorRollbackTransaction = SqlClientPrefix + "WriteTransactionRollbackError";

        private const string SqlClientPrefix = "System.Data.SqlClient.";

        private readonly TelemetryClient client;
        private readonly SqlClientDiagnosticSourceSubscriber subscriber;

        private readonly ObjectInstanceBasedOperationHolder operationHolder = new ObjectInstanceBasedOperationHolder();

        public SqlClientDiagnosticSourceListener(TelemetryConfiguration configuration)
        {
            this.client = new TelemetryClient(configuration);
            this.client.Context.GetInternalContext().SdkVersion =
                SdkVersionUtils.GetSdkVersion("rdd" + RddSource.DiagnosticSourceCore + ":");

            this.subscriber = new SqlClientDiagnosticSourceSubscriber(this);
        }

        public void Dispose()
        {
            if (this.subscriber != null)
            {
                this.subscriber.Dispose();
            }
        }
        
        void IObserver<KeyValuePair<string, object>>.OnCompleted()
        {
        }

        void IObserver<KeyValuePair<string, object>>.OnError(Exception error)
        {
        }

        void IObserver<KeyValuePair<string, object>>.OnNext(KeyValuePair<string, object> evnt)
        {
            var timestamp = DateTimeOffset.UtcNow;

            switch (evnt.Key)
            {
                case SqlBeforeExecuteCommand:
                {
                    var operationId = (Guid)CommandBefore.OperationId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);

                    var command = (SqlCommand)CommandBefore.Command.Fetch(evnt.Value);

                    if (this.operationHolder.Get(command) == null)
                    {
                        var dependencyName = string.Empty;
                        var target = string.Empty;

                        if (command.Connection != null)
                        {
                            target = string.Join(" | ", command.Connection.DataSource, command.Connection.Database);

                            var commandName = command.CommandType == CommandType.StoredProcedure
                                ? command.CommandText
                                : string.Empty;

                            dependencyName = string.IsNullOrEmpty(commandName)
                                ? string.Join(" | ", command.Connection.DataSource, command.Connection.Database)
                                : string.Join(" | ", command.Connection.DataSource, command.Connection.Database, commandName);
                        }

                        var ticks = CommandBefore.Timestamp.Fetch(evnt.Value) as long?
                                    ?? Stopwatch.GetTimestamp(); // TODO corefx#20748 - timestamp missing from event data

                            var telemetry = new DependencyTelemetry()
                        {
                            Id = operationId.ToString("N"),
                            Name = dependencyName,
                            Type = RemoteDependencyConstants.SQL,
                            Target = target,
                            Data = command.CommandText,
                            Timestamp = timestamp,
                            Duration = TimeSpan.FromTicks(ticks),
                            Success = true
                        };

                        InitializeTelemetry(telemetry, operationId);

                        this.operationHolder.Store(command, Tuple.Create(telemetry, /* isCustomCreated: */ false));
                    }

                    break;
                }

                case SqlAfterExecuteCommand:
                {
                    var operationId = (Guid)CommandAfter.OperationId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);

                    var command = (SqlCommand)CommandAfter.Command.Fetch(evnt.Value);
                    var tuple = this.operationHolder.Get(command);

                    if (tuple != null)
                    {
                        this.operationHolder.Remove(command);

                        var telemetry = tuple.Item1;

                        var ticks = (long)CommandAfter.Timestamp.Fetch(evnt.Value);

                        telemetry.Duration = TimeSpan.FromTicks(ticks) - telemetry.Duration;

                        this.client.Track(telemetry);
                    }
                    
                    break;
                }

                case SqlErrorExecuteCommand:
                {
                    var operationId = (Guid)CommandError.OperationId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);

                    var command = (SqlCommand)CommandError.Command.Fetch(evnt.Value);
                    var tuple = this.operationHolder.Get(command);

                    if (tuple != null)
                    {
                        this.operationHolder.Remove(command);

                        var telemetry = tuple.Item1;

                        var ticks = (long)CommandError.Timestamp.Fetch(evnt.Value);

                        telemetry.Duration = TimeSpan.FromTicks(ticks) - telemetry.Duration;

                        var exception = (Exception)CommandError.Exception.Fetch(evnt.Value);

                        ConfigureExceptionTelemetry(telemetry, exception);

                        this.client.Track(telemetry);
                    }

                    break;
                }
                    
                case SqlBeforeOpenConnection:
                {
                    var operationId = (Guid)ConnectionBefore.OperationId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);
                    
                    var connection = (SqlConnection)ConnectionBefore.Connection.Fetch(evnt.Value);

                    if (this.operationHolder.Get(connection) == null)
                    {
                        var operation = (string)ConnectionBefore.Operation.Fetch(evnt.Value);
                        var ticks = (long)ConnectionBefore.Timestamp.Fetch(evnt.Value);

                        var telemetry = new DependencyTelemetry()
                        {
                            Id = operationId.ToString("N"),
                            Name = string.Join(" | ", connection.DataSource, connection.Database, operation),
                            Type = RemoteDependencyConstants.SQL,
                            Target = string.Join(" | ", connection.DataSource, connection.Database),
                            Data = operation,
                            Timestamp = timestamp,
                            Duration = TimeSpan.FromTicks(ticks),
                            Success = true
                        };
                        
                        InitializeTelemetry(telemetry, operationId);

                        this.operationHolder.Store(connection, Tuple.Create(telemetry, /* isCustomCreated: */ false));
                    }

                    break;
                }

                case SqlAfterOpenConnection:
                {
                    var operationId = (Guid)ConnectionAfter.OperationId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);

                    var connection = (SqlConnection)ConnectionAfter.Connection.Fetch(evnt.Value);
                    var tuple = this.operationHolder.Get(connection);

                    if (tuple != null)
                    {
                        this.operationHolder.Remove(connection);
                    }

                    break;
                }

                case SqlErrorOpenConnection:
                {
                    var operationId = (Guid)ConnectionError.OperationId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);

                    var connection = (SqlConnection)ConnectionError.Connection.Fetch(evnt.Value);
                    var tuple = this.operationHolder.Get(connection);

                    if (tuple != null)
                    {
                        this.operationHolder.Remove(connection);

                        var telemetry = tuple.Item1;

                        var ticks = (long)ConnectionError.Timestamp.Fetch(evnt.Value);

                        telemetry.Duration = TimeSpan.FromTicks(ticks) - telemetry.Duration;

                        var exception = (Exception)ConnectionError.Exception.Fetch(evnt.Value);

                        ConfigureExceptionTelemetry(telemetry, exception);

                        this.client.Track(telemetry);
                    }

                    break;
                }

                case SqlBeforeCommitTransaction:
                case SqlBeforeRollbackTransaction:
                {
                    var operationId = (Guid)TransactionBefore.OperationId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);

                    var connection = (SqlConnection)TransactionBefore.Connection.Fetch(evnt.Value);

                    if (this.operationHolder.Get(connection) == null)
                    {
                        var operation = (string)TransactionBefore.Operation.Fetch(evnt.Value);
                        var ticks = (long)TransactionBefore.Timestamp.Fetch(evnt.Value);
                        var isolationLevel = (IsolationLevel)TransactionBefore.IsolationLevel.Fetch(evnt.Value);

                        var telemetry = new DependencyTelemetry()
                        {
                            Id = operationId.ToString("N"),
                            Name = string.Join(" | ", connection.DataSource, connection.Database, operation, isolationLevel),
                            Type = RemoteDependencyConstants.SQL,
                            Target = string.Join(" | ", connection.DataSource, connection.Database),
                            Data = operation,
                            Duration = TimeSpan.FromTicks(ticks),
                            Timestamp = timestamp,
                            Success = true
                        };

                        InitializeTelemetry(telemetry, operationId);

                        this.operationHolder.Store(connection, Tuple.Create(telemetry, /* isCustomCreated: */ false));
                    }

                    break;
                }

                case SqlAfterCommitTransaction:
                case SqlAfterRollbackTransaction:
                {
                    var operationId = (Guid)TransactionAfter.OperationId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);

                    var connection = (SqlConnection)TransactionAfter.Connection.Fetch(evnt.Value);
                    var tuple = this.operationHolder.Get(connection);

                    if (tuple != null)
                    {
                        this.operationHolder.Remove(connection);

                        var telemetry = tuple.Item1;

                        var ticks = (long)TransactionAfter.Timestamp.Fetch(evnt.Value);

                        telemetry.Duration = TimeSpan.FromTicks(ticks) - telemetry.Duration;

                        this.client.Track(telemetry);
                    }

                    break;
                }

                case SqlErrorCommitTransaction:
                case SqlErrorRollbackTransaction:
                {
                    var operationId = (Guid)TransactionError.OperationId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);

                    var connection = (SqlConnection)TransactionError.Connection.Fetch(evnt.Value);
                    var tuple = this.operationHolder.Get(connection);

                    if (tuple != null)
                    {
                        this.operationHolder.Remove(connection);

                        var telemetry = tuple.Item1;

                        var ticks = (long)TransactionError.Timestamp.Fetch(evnt.Value);

                        telemetry.Duration = TimeSpan.FromTicks(ticks) - telemetry.Duration;

                        var exception = (Exception)TransactionError.Exception.Fetch(evnt.Value);

                        ConfigureExceptionTelemetry(telemetry, exception);

                        this.client.Track(telemetry);
                    }

                    break;
                }
            }
        }

        private static void InitializeTelemetry(ITelemetry telemetry, Guid operationId)
        {
            var activity = Activity.Current;

            if (activity != null)
            {
                telemetry.Context.Operation.Id = activity.RootId;
                telemetry.Context.Operation.ParentId = activity.ParentId;

                foreach (var item in activity.Baggage)
                {
                    if (!telemetry.Context.Properties.ContainsKey(item.Key))
                    {
                        telemetry.Context.Properties[item.Key] = item.Value;
                    }
                }
            }
            else
            {
                telemetry.Context.Operation.Id = operationId.ToString("N");
            }
        }

        private static void ConfigureExceptionTelemetry(DependencyTelemetry telemetry, Exception exception)
        {
            telemetry.Success = false;
            telemetry.Properties["Exception"] = exception.ToInvariantString();

            var sqlException = exception as SqlException;

            if (sqlException != null)
            {
                telemetry.ResultCode = sqlException.Number.ToString(CultureInfo.InvariantCulture);
            }
        }

        #region Fetchers

        // Fetchers for execute command before event
        private static class CommandBefore
        {
            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
            public static readonly PropertyFetcher Command = new PropertyFetcher(nameof(Command));
            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
        }

        // Fetchers for execute command after event
        private static class CommandAfter
        {
            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
            public static readonly PropertyFetcher Command = new PropertyFetcher(nameof(Command));
            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
        }

        // Fetchers for execute command error event
        private static class CommandError
        {
            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
            public static readonly PropertyFetcher Command = new PropertyFetcher(nameof(Command));
            public static readonly PropertyFetcher Exception = new PropertyFetcher(nameof(Exception));
            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
        }

        // Fetchers for connection open/close before events
        private static class ConnectionBefore
        {
            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
            public static readonly PropertyFetcher Operation = new PropertyFetcher(nameof(Operation));
            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
        }

        // Fetchers for connection open/close after events
        private static class ConnectionAfter
        {
            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
        }

        // Fetchers for connection open/close error events
        private static class ConnectionError
        {
            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
            public static readonly PropertyFetcher Exception = new PropertyFetcher(nameof(Exception));
            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
        }

        // Fetchers for transaction commit/rollback before events
        private static class TransactionBefore
        {
            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
            public static readonly PropertyFetcher Operation = new PropertyFetcher(nameof(Operation));
            public static readonly PropertyFetcher IsolationLevel = new PropertyFetcher(nameof(IsolationLevel));
            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
        }

        // Fetchers for transaction commit/rollback after events
        private static class TransactionAfter
        {
            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
        }

        // Fetchers for transaction commit/rollback error events
        private static class TransactionError
        {
            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
            public static readonly PropertyFetcher Exception = new PropertyFetcher(nameof(Exception));
            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
        }

        #endregion

        private sealed class SqlClientDiagnosticSourceSubscriber : IObserver<DiagnosticListener>, IDisposable
        {
            private readonly SqlClientDiagnosticSourceListener sqlDiagnosticListener;
            private readonly IDisposable listenerSubscription;

            private IDisposable eventSubscription;

            internal SqlClientDiagnosticSourceSubscriber(SqlClientDiagnosticSourceListener listener)
            {
                this.sqlDiagnosticListener = listener;

                try
                {
                    this.listenerSubscription = DiagnosticListener.AllListeners.Subscribe(this);
                }
                catch (Exception ex)
                {
                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberFailedToSubscribe(ex.ToInvariantString());
                }
            }

            public void OnNext(DiagnosticListener value)
            {
                if (value != null)
                {
                    if (value.Name == DiagnosticListenerName)
                    {
                        this.eventSubscription = value.Subscribe(this.sqlDiagnosticListener);
                    }
                }
            }

            public void OnCompleted()
            {
            }

            public void OnError(Exception error)
            {
            }

            public void Dispose()
            {
                if (this.eventSubscription != null)
                {
                    this.eventSubscription.Dispose();
                }

                if (this.listenerSubscription != null)
                {
                    this.listenerSubscription.Dispose();
                }
            }
        }
    }
}