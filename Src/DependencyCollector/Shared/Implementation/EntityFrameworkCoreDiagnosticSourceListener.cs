namespace Microsoft.ApplicationInsights.DependencyCollector.Implementation
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using Microsoft.ApplicationInsights.Channel;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.ApplicationInsights.Extensibility.Implementation;
    using Microsoft.ApplicationInsights.Extensibility.Implementation.Tracing;
    using Microsoft.ApplicationInsights.Web.Implementation;
    using System.Data.Common;

    internal class EntityFrameworkCoreDiagnosticSourceListener : IObserver<KeyValuePair<string, object>>, IDisposable
    {
        // Event ids defined at: https://github.com/aspnet/EntityFramework/blob/dev/src/EFCore.Relational/Diagnostics/RelationalEventId.cs
        public const string DiagnosticListenerName = "Microsoft.EntityFrameworkCore";

        public const string CommandExecutedEventId = EntityFrameworkCorePrefix + "Command.CommandExecuted";
        public const string CommandErrorEventId = EntityFrameworkCorePrefix + "Command.CommandError";
        
        public const string SqlAfterOpenConnection = EntityFrameworkCorePrefix + "WriteConnectionOpenAfter";
        public const string SqlErrorOpenConnection = EntityFrameworkCorePrefix + "WriteConnectionOpenError";

        public const string SqlAfterCloseConnection = EntityFrameworkCorePrefix + "WriteConnectionCloseAfter";
        public const string SqlErrorCloseConnection = EntityFrameworkCorePrefix + "WriteConnectionCloseError";

        public const string SqlAfterCommitTransaction = EntityFrameworkCorePrefix + "WriteTransactionCommitAfter";
        public const string SqlErrorCommitTransaction = EntityFrameworkCorePrefix + "WriteTransactionCommitError";

        public const string SqlAfterRollbackTransaction = EntityFrameworkCorePrefix + "WriteTransactionRollbackAfter";
        public const string SqlErrorRollbackTransaction = EntityFrameworkCorePrefix + "WriteTransactionRollbackError";

        private const string EntityFrameworkCorePrefix = "Microsoft.EntityFrameworkCore.Database.";

        private readonly TelemetryClient client;
        private readonly EntityFrameworkCoreDiagnosticSourceSubscriber subscriber;

        public EntityFrameworkCoreDiagnosticSourceListener(TelemetryConfiguration configuration)
        {
            this.client = new TelemetryClient(configuration);
            this.client.Context.GetInternalContext().SdkVersion =
                SdkVersionUtils.GetSdkVersion("rdd" + RddSource.DiagnosticSourceCore + ":");

            this.subscriber = new EntityFrameworkCoreDiagnosticSourceSubscriber(this);
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
            switch (evnt.Key)
            {
                case CommandExecutedEventId:
                {
                    var commandId = (Guid)CommandExecuted.CommandId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(commandId, evnt.Key);
                    
                    this.OnCommandExecutedEvent(
                        commandId,
                        CommandExecuted.ExecuteMethod.Fetch(evnt.Value).ToString(),
                        (Guid)CommandExecuted.ConnectionId.Fetch(evnt.Value),
                        (SqlCommand)CommandExecuted.Command.Fetch(evnt.Value),
                        (bool)CommandExecuted.IsAsync.Fetch(evnt.Value),
                        (DateTimeOffset)CommandExecuted.StartTime.Fetch(evnt.Value),
                        (TimeSpan)CommandExecuted.Duration.Fetch(evnt.Value));

                    break;
                }

//                case CommandErrorEventId:
//                {
//                    var operationId = (Guid)CommandError.OperationId.Fetch(evnt.Value);
//
//                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);
//                    
//                    this.OnCommandErrorEvent(
//                        operationId,
//                        (string)CommandError.Operation.Fetch(evnt.Value),
//                        (Guid)CommandError.ConnectionId.Fetch(evnt.Value),
//                        (Exception)CommandError.Exception.Fetch(evnt.Value),
//                        (long)CommandError.Timestamp.Fetch(evnt.Value));
//
//                    break;
//                }
//
//                case SqlAfterOpenConnection:
//                case SqlAfterCloseConnection:
//                {
//                    var operationId = (Guid)ConnectionAfter.OperationId.Fetch(evnt.Value);
//
//                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);
//                    
//                    this.OnAfterConnectionEvent(
//                        operationId,
//                        (string)ConnectionAfter.Operation.Fetch(evnt.Value),
//                        (Guid)ConnectionAfter.ConnectionId.Fetch(evnt.Value),
//                        (SqlConnection)ConnectionAfter.Connection.Fetch(evnt.Value),
//                        (IDictionary)ConnectionAfter.Statistics.Fetch(evnt.Value),
//                        (long)ConnectionAfter.Timestamp.Fetch(evnt.Value));
//
//                    break;
//                }
//
//                case SqlErrorOpenConnection:
//                case SqlErrorCloseConnection:
//                {
//                    var operationId = (Guid)ConnectionError.OperationId.Fetch(evnt.Value);
//
//                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);
//                    
//                    this.OnErrorConnectionEvent(
//                        operationId,
//                        (string)ConnectionError.Operation.Fetch(evnt.Value),
//                        (Guid)ConnectionError.ConnectionId.Fetch(evnt.Value),
//                        (Exception)ConnectionError.Exception.Fetch(evnt.Value),
//                        (long)ConnectionError.Timestamp.Fetch(evnt.Value));
//
//                    break;
//                }
//
//                case SqlAfterCommitTransaction:
//                case SqlAfterRollbackTransaction:
//                {
//                    var operationId = (Guid)TransactionAfter.OperationId.Fetch(evnt.Value);
//
//                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);
//                    
//                    this.OnAfterTransactionEvent(
//                        operationId,
//                        (string)TransactionAfter.Operation.Fetch(evnt.Value),
//                        (IsolationLevel)TransactionAfter.IsolationLevel.Fetch(evnt.Value),
//                        (SqlConnection)TransactionAfter.Connection.Fetch(evnt.Value),
//                        (long)TransactionAfter.Timestamp.Fetch(evnt.Value));
//
//                    break;
//                }
//
//                case SqlErrorCommitTransaction:
//                case SqlErrorRollbackTransaction:
//                {
//                    var operationId = (Guid)TransactionError.OperationId.Fetch(evnt.Value);
//
//                    DependencyCollectorEventSource.Log.SqlClientDiagnosticSubscriberCallbackCalled(operationId, evnt.Key);
//                    
//                    this.OnErrorTransactionEvent(
//                        operationId,
//                        (string)TransactionError.Operation.Fetch(evnt.Value),
//                        (IsolationLevel)TransactionError.IsolationLevel.Fetch(evnt.Value),
//                        (SqlConnection)TransactionError.Connection.Fetch(evnt.Value),
//                        (Exception)TransactionError.Exception.Fetch(evnt.Value),
//                        (long)TransactionError.Timestamp.Fetch(evnt.Value));
//
//                    break;
//                }
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

        private static void CorrelateErrorTelemetry(ExceptionTelemetry telemetry, Guid operationId)
        {
            telemetry.Context.Operation.ParentId = operationId.ToString("N");
        }

        private void OnCommandExecutedEvent(
            Guid commandId,
            string executeMethod,
            Guid connectionId,
            DbCommand command,
            bool @async,
            DateTimeOffset startTime,
            TimeSpan duration)
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

            var telemetry = new DependencyTelemetry()
            {
                Id = commandId.ToString("N"),
                Name = dependencyName,
                Type = RemoteDependencyConstants.SQL,
                Target = target,
                Data = command.CommandText,
                Duration = duration,
                Timestamp = DateTimeOffset.UtcNow - duration,
                Success = true
            };

            InitializeTelemetry(telemetry, commandId);
            
            this.client.Track(telemetry);
        }

//        private void OnCommandErrorEvent(
//            Guid operationId,
//            string operation,
//            Guid connectionId,
//            Exception exception,
//            long endTimestamp)
//        {
//            var duration = this.DeriveDuration(operationId, endTimestamp);
//
//            var telemetry = new ExceptionTelemetry(exception)
//            {
//                Message = exception.Message,
//                Timestamp = DateTimeOffset.UtcNow - duration,
//                SeverityLevel = SeverityLevel.Critical
//            };
//
//            InitializeTelemetry(telemetry, operationId);
//            CorrelateErrorTelemetry(telemetry, operationId);
//
//            this.client.Track(telemetry);
//        }
//
//        private void OnAfterConnectionEvent(
//            Guid operationId, 
//            string operation,
//            Guid connectionId,
//            SqlConnection connection, 
//            IDictionary statistics, 
//            long endTimestamp)
//        {
//            var duration = this.DeriveDuration(operationId, endTimestamp);
//            
//            var telemetry = new DependencyTelemetry()
//            {
//                Id = operationId.ToString("N"),
//                Name = string.Join(" | ", connection.DataSource, connection.Database, operation),
//                Type = RemoteDependencyConstants.SQL,
//                Target = string.Join(" | ", connection.DataSource, connection.Database),
//                Data = operation,
//                Duration = duration,
//                Timestamp = DateTimeOffset.UtcNow - duration,
//                Success = true
//            };
//
//            InitializeTelemetry(telemetry, operationId);
//
//            this.client.Track(telemetry);
//        }
//
//        private void OnErrorConnectionEvent(
//            Guid operationId,
//            string operation,
//            Guid connectionId,
//            Exception exception,
//            long endTimestamp)
//        {
//            var duration = this.DeriveDuration(operationId, endTimestamp);
//
//            var telemetry = new ExceptionTelemetry(exception)
//            {
//                Message = exception.Message,
//                Timestamp = DateTimeOffset.UtcNow - duration,
//                SeverityLevel = SeverityLevel.Critical
//            };
//
//            InitializeTelemetry(telemetry, operationId);
//            CorrelateErrorTelemetry(telemetry, operationId);
//
//            this.client.Track(telemetry);
//        }
//
//        private void OnAfterTransactionEvent(
//            Guid operationId,
//            string operation,
//            IsolationLevel isolationLevel,
//            SqlConnection connection,
//            long endTimestamp)
//        {
//            var duration = this.DeriveDuration(operationId, endTimestamp);
//
//            var telemetry = new DependencyTelemetry()
//            {
//                Id = operationId.ToString("N"),
//                Name = string.Join(" | ", connection.DataSource, connection.Database, operation, isolationLevel),
//                Type = RemoteDependencyConstants.SQL,
//                Target = string.Join(" | ", connection.DataSource, connection.Database),
//                Data = operation,
//                Duration = duration,
//                Timestamp = DateTimeOffset.UtcNow - duration,
//                Success = true
//            };
//
//            InitializeTelemetry(telemetry, operationId);
//
//            this.client.Track(telemetry);
//        }
//
//        private void OnErrorTransactionEvent(
//            Guid operationId,
//            string operation,
//            IsolationLevel isolationLevel,
//            SqlConnection connection,
//            Exception exception,
//            long endTimestamp)
//        {
//            var duration = this.DeriveDuration(operationId, endTimestamp);
//
//            var telemetry = new ExceptionTelemetry(exception)
//            {
//                Message = exception.Message,
//                Timestamp = DateTimeOffset.UtcNow - duration,
//                SeverityLevel = SeverityLevel.Critical
//            };
//
//            InitializeTelemetry(telemetry, operationId);
//            CorrelateErrorTelemetry(telemetry, operationId);
//
//            this.client.Track(telemetry);
//        }

        #region Fetchers
        
        // Fetchers for execute command after event
        private static class CommandExecuted
        {
            public static readonly PropertyFetcher CommandId = new PropertyFetcher(nameof(CommandId));
            public static readonly PropertyFetcher ExecuteMethod = new PropertyFetcher(nameof(ExecuteMethod));
            public static readonly PropertyFetcher ConnectionId = new PropertyFetcher(nameof(ConnectionId));
            public static readonly PropertyFetcher Command = new PropertyFetcher(nameof(Command));
            public static readonly PropertyFetcher IsAsync = new PropertyFetcher(nameof(IsAsync));
            public static readonly PropertyFetcher StartTime = new PropertyFetcher(nameof(StartTime));
            public static readonly PropertyFetcher Duration = new PropertyFetcher(nameof(Duration));
        }

//        // Fetchers for execute command error event
//        private static class CommandError
//        {
//            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
//            public static readonly PropertyFetcher Operation = new PropertyFetcher(nameof(Operation));
//            public static readonly PropertyFetcher ConnectionId = new PropertyFetcher(nameof(ConnectionId));
//            public static readonly PropertyFetcher Exception = new PropertyFetcher(nameof(Exception));
//            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
//        }
//
//        // Fetchers for connection open/close after events
//        private static class ConnectionAfter
//        {
//            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
//            public static readonly PropertyFetcher Operation = new PropertyFetcher(nameof(Operation));
//            public static readonly PropertyFetcher ConnectionId = new PropertyFetcher(nameof(ConnectionId));
//            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
//            public static readonly PropertyFetcher Statistics = new PropertyFetcher(nameof(Statistics));
//            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
//        }
//
//        // Fetchers for connection open/close error events
//        private static class ConnectionError
//        {
//            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
//            public static readonly PropertyFetcher Operation = new PropertyFetcher(nameof(Operation));
//            public static readonly PropertyFetcher ConnectionId = new PropertyFetcher(nameof(ConnectionId));
//            public static readonly PropertyFetcher Exception = new PropertyFetcher(nameof(Exception));
//            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
//        }
//
//        // Fetchers for transaction commit/rollback after events
//        private static class TransactionAfter
//        {
//            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
//            public static readonly PropertyFetcher Operation = new PropertyFetcher(nameof(Operation));
//            public static readonly PropertyFetcher IsolationLevel = new PropertyFetcher(nameof(IsolationLevel));
//            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
//            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
//        }
//
//        // Fetchers for transaction commit/rollback error events
//        private static class TransactionError
//        {
//            public static readonly PropertyFetcher OperationId = new PropertyFetcher(nameof(OperationId));
//            public static readonly PropertyFetcher Operation = new PropertyFetcher(nameof(Operation));
//            public static readonly PropertyFetcher IsolationLevel = new PropertyFetcher(nameof(IsolationLevel));
//            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
//            public static readonly PropertyFetcher Exception = new PropertyFetcher(nameof(Exception));
//            public static readonly PropertyFetcher Timestamp = new PropertyFetcher(nameof(Timestamp));
//        }

        #endregion

        private sealed class EntityFrameworkCoreDiagnosticSourceSubscriber : IObserver<DiagnosticListener>, IDisposable
        {
            private readonly EntityFrameworkCoreDiagnosticSourceListener efDiagnosticListener;
            private readonly IDisposable listenerSubscription;

            private IDisposable eventSubscription;

            internal EntityFrameworkCoreDiagnosticSourceSubscriber(EntityFrameworkCoreDiagnosticSourceListener listener)
            {
                this.efDiagnosticListener = listener;

                try
                {
                    this.listenerSubscription = DiagnosticListener.AllListeners.Subscribe(this);
                }
                catch (Exception ex)
                {
                    DependencyCollectorEventSource.Log.EntityFrameworkCoreDiagnosticSubscriberFailedToSubscribe(ex.ToInvariantString());
                }
            }

            public void OnNext(DiagnosticListener value)
            {
                if (value != null)
                {
                    if (value.Name == DiagnosticListenerName)
                    {
                        this.eventSubscription = value.Subscribe(this.efDiagnosticListener);
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