namespace Microsoft.ApplicationInsights.DependencyCollector.Implementation
{
    using System;
    using System.Collections.Generic;
    using System.Data;
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
        
        public const string ConnectionOpenedEventId = EntityFrameworkCorePrefix + "Connection.ConnectionOpened";
        public const string ConnectionClosedEventId = EntityFrameworkCorePrefix + "Connection.ConnectionClosed";
        public const string ConnectionErrorEventId = EntityFrameworkCorePrefix + "Connection.ConnectionError";
        
        public const string TransactionCommittedEventId = EntityFrameworkCorePrefix + "Transaction.TransactionCommitted";
        public const string TransactionRolledBackEventId = EntityFrameworkCorePrefix + "Transaction.TransactionRolledBack";
        public const string TransactionErrorEventId = EntityFrameworkCorePrefix + "Transaction.TransactionError";

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

                    DependencyCollectorEventSource.Log.EntityFrameworkCoreDiagnosticSubscriberCallbackCalled(commandId, evnt.Key);
                    
                    this.OnCommandExecutedEvent(
                        commandId,
                        (DbCommand)CommandExecuted.Command.Fetch(evnt.Value),
                        (DateTimeOffset)CommandExecuted.StartTime.Fetch(evnt.Value),
                        (TimeSpan)CommandExecuted.Duration.Fetch(evnt.Value));

                    break;
                }

                case CommandErrorEventId:
                {
                    var commandId = (Guid)CommandError.CommandId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.EntityFrameworkCoreDiagnosticSubscriberCallbackCalled(commandId, evnt.Key);
                    
                    this.OnCommandErrorEvent(
                        commandId,
                        (Exception)CommandError.Exception.Fetch(evnt.Value),
                        (DateTimeOffset)CommandError.StartTime.Fetch(evnt.Value));

                    break;
                }

                case ConnectionOpenedEventId:
                {
                    var connectionId = (Guid)ConnectionEnd.ConnectionId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.EntityFrameworkCoreDiagnosticSubscriberCallbackCalled(connectionId, evnt.Key);
                    
                    this.OnConnectionEndEvent(
                        connectionId,
                        "Open",
                        (DbConnection)ConnectionEnd.Connection.Fetch(evnt.Value),
                        (DateTimeOffset)ConnectionEnd.StartTime.Fetch(evnt.Value),
                        (TimeSpan)ConnectionEnd.Duration.Fetch(evnt.Value));

                    break;
                }
                    
                case ConnectionClosedEventId:
                {
                    var connectionId = (Guid)ConnectionEnd.ConnectionId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.EntityFrameworkCoreDiagnosticSubscriberCallbackCalled(connectionId, evnt.Key);
                    
                    this.OnConnectionEndEvent(
                        connectionId,
                        "Close",
                        (DbConnection)ConnectionEnd.Connection.Fetch(evnt.Value),
                        (DateTimeOffset)ConnectionEnd.StartTime.Fetch(evnt.Value),
                        (TimeSpan)ConnectionEnd.Duration.Fetch(evnt.Value));

                    break;
                }

                case ConnectionErrorEventId:
                {
                    var connectionId = (Guid)ConnectionError.ConnectionId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.EntityFrameworkCoreDiagnosticSubscriberCallbackCalled(connectionId, evnt.Key);
                    
                    this.OnConnectionErrorEvent(
                        connectionId,
                        (Exception)ConnectionError.Exception.Fetch(evnt.Value),
                        (DateTimeOffset)ConnectionError.StartTime.Fetch(evnt.Value));

                    break;
                }

                case TransactionCommittedEventId:
                {
                    var transactionId = (Guid)TransactionEnd.TransactionId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.EntityFrameworkCoreDiagnosticSubscriberCallbackCalled(transactionId, evnt.Key);
                    
                    this.OnTransactionEndEvent(
                        transactionId,
                        "Commit",
                        (DbTransaction)TransactionEnd.Transaction.Fetch(evnt.Value),
                        (DateTimeOffset)TransactionEnd.StartTime.Fetch(evnt.Value),
                        (TimeSpan)TransactionEnd.Duration.Fetch(evnt.Value));

                    break;
                }

                case TransactionRolledBackEventId:
                {
                    var transactionId = (Guid)TransactionEnd.TransactionId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.EntityFrameworkCoreDiagnosticSubscriberCallbackCalled(transactionId, evnt.Key);
                    
                    this.OnTransactionEndEvent(
                        transactionId,
                        "Rollback",
                        (DbTransaction)TransactionEnd.Transaction.Fetch(evnt.Value),
                        (DateTimeOffset)TransactionEnd.StartTime.Fetch(evnt.Value),
                        (TimeSpan)TransactionEnd.Duration.Fetch(evnt.Value));

                    break;
                }

                case TransactionErrorEventId:
                {
                    var transactionId = (Guid)TransactionError.TransactionId.Fetch(evnt.Value);

                    DependencyCollectorEventSource.Log.EntityFrameworkCoreDiagnosticSubscriberCallbackCalled(transactionId, evnt.Key);
                    
                    this.OnTransactionErrorEvent(
                        transactionId,
                        (Exception)TransactionError.Exception.Fetch(evnt.Value),
                        (DateTimeOffset)TransactionError.StartTime.Fetch(evnt.Value));

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

        private static void CorrelateErrorTelemetry(ExceptionTelemetry telemetry, Guid operationId)
        {
            telemetry.Context.Operation.ParentId = operationId.ToString("N");
        }

        private void OnCommandExecutedEvent(
            Guid commandId,
            DbCommand command,
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
                Timestamp = startTime,
                Success = true
            };

            InitializeTelemetry(telemetry, commandId);
            
            this.client.Track(telemetry);
        }

        private void OnCommandErrorEvent(
            Guid commandId,
            Exception exception,
            DateTimeOffset startTime)
        {
            var telemetry = new ExceptionTelemetry(exception)
            {
                Message = exception.Message,
                Timestamp = startTime,
                SeverityLevel = SeverityLevel.Critical
            };

            InitializeTelemetry(telemetry, commandId);
            CorrelateErrorTelemetry(telemetry, commandId);

            this.client.Track(telemetry);
        }

        private void OnConnectionEndEvent(
            Guid connectionId,
            string operation,
            DbConnection connection,
            DateTimeOffset startTime,
            TimeSpan duration)
        {
            var telemetry = new DependencyTelemetry()
            {
                Id = connectionId.ToString("N"),
                Name = string.Join(" | ", connection.DataSource, connection.Database, operation),
                Type = RemoteDependencyConstants.SQL,
                Target = string.Join(" | ", connection.DataSource, connection.Database),
                Data = operation,
                Duration = duration,
                Timestamp = startTime,
                Success = true
            };

            InitializeTelemetry(telemetry, connectionId);

            this.client.Track(telemetry);
        }

        private void OnConnectionErrorEvent(
            Guid connectionId,
            Exception exception,
            DateTimeOffset startTime)
        {
            var telemetry = new ExceptionTelemetry(exception)
            {
                Message = exception.Message,
                Timestamp = startTime,
                SeverityLevel = SeverityLevel.Critical
            };

            InitializeTelemetry(telemetry, connectionId);
            CorrelateErrorTelemetry(telemetry, connectionId);

            this.client.Track(telemetry);
        }

        private void OnTransactionEndEvent(
            Guid transactionId,
            string operation,
            DbTransaction transaction,
            DateTimeOffset startTime,
            TimeSpan duration)
        {
            var connection = transaction.Connection;
            
            var telemetry = new DependencyTelemetry()
            {
                Id = transactionId.ToString("N"),
                Name = string.Join(" | ", connection.DataSource, connection.Database, operation, transaction.IsolationLevel),
                Type = RemoteDependencyConstants.SQL,
                Target = string.Join(" | ", connection.DataSource, connection.Database),
                Data = operation,
                Duration = duration,
                Timestamp = startTime,
                Success = true
            };

            InitializeTelemetry(telemetry, transactionId);

            this.client.Track(telemetry);
        }

        private void OnTransactionErrorEvent(
            Guid transactionId,
            Exception exception,
            DateTimeOffset startTime)
        {
            var telemetry = new ExceptionTelemetry(exception)
            {
                Message = exception.Message,
                Timestamp = startTime,
                SeverityLevel = SeverityLevel.Critical
            };

            InitializeTelemetry(telemetry, transactionId);
            CorrelateErrorTelemetry(telemetry, transactionId);

            this.client.Track(telemetry);
        }

        #region Fetchers
        
        // Fetchers for execute command after event
        private static class CommandExecuted
        {
            public static readonly PropertyFetcher CommandId = new PropertyFetcher(nameof(CommandId));
            public static readonly PropertyFetcher Command = new PropertyFetcher(nameof(Command));
            public static readonly PropertyFetcher StartTime = new PropertyFetcher(nameof(StartTime));
            public static readonly PropertyFetcher Duration = new PropertyFetcher(nameof(Duration));
        }

        // Fetchers for execute command error event
        private static class CommandError
        {
            public static readonly PropertyFetcher CommandId = new PropertyFetcher(nameof(CommandId));
            public static readonly PropertyFetcher Command = new PropertyFetcher(nameof(Command));
            public static readonly PropertyFetcher StartTime = new PropertyFetcher(nameof(StartTime));
            public static readonly PropertyFetcher Exception = new PropertyFetcher(nameof(Exception));
        }

        // Fetchers for connection open/close after events
        private static class ConnectionEnd
        {
            public static readonly PropertyFetcher ConnectionId = new PropertyFetcher(nameof(ConnectionId));
            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
            public static readonly PropertyFetcher StartTime = new PropertyFetcher(nameof(StartTime));
            public static readonly PropertyFetcher Duration = new PropertyFetcher(nameof(Duration));
        }

        // Fetchers for connection open/close error events
        private static class ConnectionError
        {
            public static readonly PropertyFetcher ConnectionId = new PropertyFetcher(nameof(ConnectionId));
            public static readonly PropertyFetcher Connection = new PropertyFetcher(nameof(Connection));
            public static readonly PropertyFetcher StartTime = new PropertyFetcher(nameof(StartTime));
            public static readonly PropertyFetcher Exception = new PropertyFetcher(nameof(Exception));
        }

        // Fetchers for transaction commit/rollback after events
        private static class TransactionEnd
        {
            public static readonly PropertyFetcher TransactionId = new PropertyFetcher(nameof(TransactionId));
            public static readonly PropertyFetcher Transaction = new PropertyFetcher(nameof(Transaction));
            public static readonly PropertyFetcher StartTime = new PropertyFetcher(nameof(StartTime));
            public static readonly PropertyFetcher Duration = new PropertyFetcher(nameof(Duration));
        }

        // Fetchers for transaction commit/rollback error events
        private static class TransactionError
        {
            public static readonly PropertyFetcher TransactionId = new PropertyFetcher(nameof(TransactionId));
            public static readonly PropertyFetcher StartTime = new PropertyFetcher(nameof(StartTime));
            public static readonly PropertyFetcher Exception = new PropertyFetcher(nameof(Exception));
        }

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