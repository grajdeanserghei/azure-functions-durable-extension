using System;
using System.Collections.Generic;
using System.Text;
using DurableTask.AzureStorage;
using DurableTask.Core;
using DurableTask.Core.Tracking;
using DurableTask.ServiceBus;
using DurableTask.ServiceBus.Settings;
using DurableTask.ServiceBus.Tracking;
using Microsoft.Azure.WebJobs.Extensions.DurableTask.Options;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask
{
    public class OrchestrationServiceFactory : IOrchestrationServiceFactory
    {
        private IOrchestrationServiceFactory innerFactory;

        public OrchestrationServiceFactory(
            IOptions<DurableTaskOptions> options,
            IConnectionStringResolver connectionStringResolver)
        {
            var configuredProvider = options.Value.StorageProvider.GetConfiguredProvider();
            switch (configuredProvider)
            {
                case AzureStorageOptions azureStorageOptions:
                    this.innerFactory = new AzureStorageOrchestrationServiceFactory(options.Value, connectionStringResolver);
                    break;
                case ServiceBusOptions serviceBusOptions:
                    this.innerFactory = new ServiceBusOrchestrationServiceFactory(options.Value, connectionStringResolver);
                    break;
                default:
                    throw new InvalidOperationException($"{configuredProvider.GetType()} is not a supported storage provider.");
            }
        }

        public IOrchestrationService GetOrchestrationService()
        {
            return this.innerFactory.GetOrchestrationService();
        }

        public IOrchestrationServiceClient GetOrchestrationClient(OrchestrationClientAttribute attribute)
        {
            return this.innerFactory.GetOrchestrationClient(attribute);
        }

        private static StorageAccountDetails GetStorageAccountDetailsOrNull(IConnectionStringResolver connectionStringResolver, string connectionName)
        {
            if (string.IsNullOrEmpty(connectionName))
            {
                return null;
            }

            string resolvedStorageConnectionString = connectionStringResolver.Resolve(connectionName);
            if (string.IsNullOrEmpty(resolvedStorageConnectionString))
            {
                throw new InvalidOperationException($"Unable to resolve the Azure Storage connection named '{connectionName}'.");
            }

            return new StorageAccountDetails
            {
                ConnectionString = resolvedStorageConnectionString,
            };
        }

        private class AzureStorageOrchestrationServiceFactory : IOrchestrationServiceFactory
        {
            private readonly DurableTaskOptions options;
            private readonly IConnectionStringResolver connectionStringResolver;
            private readonly AzureStorageOrchestrationService defaultService;
            private readonly AzureStorageOrchestrationServiceSettings defaultSettings;

            public AzureStorageOrchestrationServiceFactory(
                DurableTaskOptions options,
                IConnectionStringResolver connectionStringResolver)
            {
                this.options = options;
                this.connectionStringResolver = connectionStringResolver;
                this.defaultSettings = this.GetAzureStorageOrchestrationServiceSettings(options.StorageProvider.AzureStorage);
                this.defaultService = new AzureStorageOrchestrationService(this.defaultSettings);
            }

            public IOrchestrationService GetOrchestrationService()
            {
                return this.defaultService;
            }

            public IOrchestrationServiceClient GetOrchestrationClient(OrchestrationClientAttribute attribute)
            {
                return this.GetAzureStorageOrchestrationService(attribute);
            }

            private AzureStorageOrchestrationService GetAzureStorageOrchestrationService(OrchestrationClientAttribute attribute)
            {
                AzureStorageOrchestrationServiceSettings settings = this.GetOrchestrationServiceSettings(attribute);

                AzureStorageOrchestrationService innerClient;
                if (string.Equals(this.defaultSettings.TaskHubName, settings.TaskHubName, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(this.defaultSettings.StorageConnectionString, settings.StorageConnectionString, StringComparison.OrdinalIgnoreCase))
                {
                    // It's important that clients use the same AzureStorageOrchestrationService instance
                    // as the host when possible to ensure we any send operations can be picked up
                    // immediately instead of waiting for the next queue polling interval.
                    innerClient = this.defaultService;
                }
                else
                {
                    innerClient = new AzureStorageOrchestrationService(settings);
                }

                return innerClient;
            }

            internal AzureStorageOrchestrationServiceSettings GetOrchestrationServiceSettings(OrchestrationClientAttribute attribute)
            {
                return this.GetAzureStorageOrchestrationServiceSettings(
                    this.options.StorageProvider.AzureStorage,
                    connectionNameOverride: attribute.ConnectionName,
                    taskHubNameOverride: attribute.TaskHub);
            }

            internal AzureStorageOrchestrationServiceSettings GetAzureStorageOrchestrationServiceSettings(
                AzureStorageOptions azureStorageOptions,
                string connectionNameOverride = null,
                string taskHubNameOverride = null)
            {
                string connectionName = connectionNameOverride ?? azureStorageOptions.ConnectionStringName ?? ConnectionStringNames.Storage;
                string resolvedStorageConnectionString = this.connectionStringResolver.Resolve(connectionName);

                if (string.IsNullOrEmpty(resolvedStorageConnectionString))
                {
                    throw new InvalidOperationException("Unable to find an Azure Storage connection string to use for this binding.");
                }

                TimeSpan extendedSessionTimeout = TimeSpan.FromSeconds(
                    Math.Max(this.options.ExtendedSessionIdleTimeoutInSeconds, 0));

                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    StorageConnectionString = resolvedStorageConnectionString,
                    TaskHubName = taskHubNameOverride ?? azureStorageOptions.HubName,
                    PartitionCount = azureStorageOptions.PartitionCount,
                    ControlQueueBatchSize = azureStorageOptions.ControlQueueBatchSize,
                    ControlQueueVisibilityTimeout = azureStorageOptions.ControlQueueVisibilityTimeout,
                    WorkItemQueueVisibilityTimeout = azureStorageOptions.WorkItemQueueVisibilityTimeout,
                    MaxConcurrentTaskOrchestrationWorkItems = this.options.MaxConcurrentOrchestratorFunctions,
                    MaxConcurrentTaskActivityWorkItems = this.options.MaxConcurrentActivityFunctions,
                    ExtendedSessionsEnabled = this.options.ExtendedSessionsEnabled,
                    ExtendedSessionIdleTimeout = extendedSessionTimeout,
                    MaxQueuePollingInterval = azureStorageOptions.MaxQueuePollingInterval,
                    TrackingStoreStorageAccountDetails = GetStorageAccountDetailsOrNull(
                        this.connectionStringResolver,
                        azureStorageOptions.TrackingStoreConnectionStringName),
                };

                if (!string.IsNullOrEmpty(azureStorageOptions.TrackingStoreNamePrefix))
                {
                    settings.TrackingStoreNamePrefix = azureStorageOptions.TrackingStoreNamePrefix;
                }

                return settings;
            }
        }

        private class ServiceBusOrchestrationServiceFactory : IOrchestrationServiceFactory
        {
            private readonly DurableTaskOptions options;
            private readonly IConnectionStringResolver connectionStringResolver;

            public ServiceBusOrchestrationServiceFactory(
                DurableTaskOptions options,
                IConnectionStringResolver connectionStringResolver)
            {
                this.options = options;
                this.connectionStringResolver = connectionStringResolver;
            }

            public IOrchestrationServiceClient GetOrchestrationClient(OrchestrationClientAttribute attribute)
            {
                string connectionStringName = attribute.ConnectionName;
                if (connectionStringName == null)
                {
                    throw new InvalidOperationException("Cannot get ServiceBus orchestration service without a ServiceBus connection string");
                }

                string connectionString = this.connectionStringResolver.Resolve(connectionStringName);
                if (connectionString == null)
                {
                    throw new InvalidOperationException($"The connection string name {connectionStringName} was not able to be resolved");
                }

                return (IOrchestrationServiceClient)this.GetOrchestrationService(connectionString, attribute.TaskHub);
            }

            public IOrchestrationService GetOrchestrationService()
            {
                string connectionStringName = this.options.StorageProvider.ServiceBus.ConnectionStringName;
                if (connectionStringName == null)
                {
                    throw new InvalidOperationException("Cannot get ServiceBus orchestration service without a ServiceBus connection string");
                }

                string connectionString = this.connectionStringResolver.Resolve(connectionStringName);
                if (connectionString == null)
                {
                    throw new InvalidOperationException($"The connection string name {connectionStringName} was not able to be resolved");
                }

                return this.GetOrchestrationService(connectionString, this.options.GetTaskHubName());
            }

            private IOrchestrationService GetOrchestrationService(string connectionString, string taskHubName)
            {
                var settings = GetOrchestrationServiceSettings(this.options.StorageProvider.ServiceBus);
                var instanceStore = this.GetOrchestrationServiceInstanceStore(taskHubName);
                var blobStore = this.GetOrchestrationServiceBlobStore(taskHubName);
                return new ServiceBusOrchestrationService(connectionString, this.options.GetTaskHubName(), instanceStore, blobStore, settings);
            }

            private IOrchestrationServiceInstanceStore GetOrchestrationServiceInstanceStore(string taskHubName)
            {
                string instanceTableConnectionStringName = this.options.StorageProvider.ServiceBus.InstanceTableConnectionStringName;
                if (instanceTableConnectionStringName == null)
                {
                    throw new InvalidOperationException("Cannot get ServiceBus orchestration service without a instance table connection string");
                }

                string instanceTableConnectionString = this.connectionStringResolver.Resolve(instanceTableConnectionStringName);
                if (instanceTableConnectionString == null)
                {
                    throw new InvalidOperationException($"The connection string name {instanceTableConnectionString} was not able to be resolved");
                }

                return new AzureTableInstanceStore(taskHubName, instanceTableConnectionString);
            }

            private IOrchestrationServiceBlobStore GetOrchestrationServiceBlobStore(string taskHubName)
            {
                string blobConnectionStringName = this.options.StorageProvider.ServiceBus.BlobConnectionStringName;
                if (blobConnectionStringName == null)
                {
                    throw new InvalidOperationException("Cannot get ServiceBus orchestration service without a instance table connection string");
                }

                string blobConnectionString = this.connectionStringResolver.Resolve(blobConnectionStringName);
                if (blobConnectionString == null)
                {
                    throw new InvalidOperationException($"The connection string name {blobConnectionStringName} was not able to be resolved");
                }

                return new AzureStorageBlobStore(taskHubName, blobConnectionString);
            }

            private static ServiceBusOrchestrationServiceSettings GetOrchestrationServiceSettings(ServiceBusOptions options)
            {
                ServiceBusOrchestrationServiceSettings settings = new ServiceBusOrchestrationServiceSettings();
                if (options.MaxQueueSizeInMegabytes.HasValue)
                {
                    settings.MaxQueueSizeInMegabytes = options.MaxQueueSizeInMegabytes.Value;
                }

                if (options.MaxTaskActivityDeliveryCount.HasValue)
                {
                    settings.MaxTaskActivityDeliveryCount = options.MaxTaskActivityDeliveryCount.Value;
                }

                if (options.MaxTaskOrchestrationDeliveryCount.HasValue)
                {
                    settings.MaxTaskOrchestrationDeliveryCount = options.MaxTaskOrchestrationDeliveryCount.Value;
                }

                if (options.MaxTrackingDeliveryCount.HasValue)
                {
                    settings.MaxTrackingDeliveryCount = options.MaxTrackingDeliveryCount.Value;
                }

                return settings;
            }
        }
    }
}
