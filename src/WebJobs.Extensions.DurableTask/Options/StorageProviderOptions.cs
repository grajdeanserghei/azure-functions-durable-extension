using System;
using System.Linq;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask.Options
{
    public class StorageProviderOptions
    {
        private CommonStorageProviderOptions configuredProvider;

        public AzureStorageOptions AzureStorage { get; set; }

        public ServiceBusOptions ServiceBus { get; set; }

        internal CommonStorageProviderOptions GetConfiguredProvider()
        {
            if (this.configuredProvider == null)
            {
                var storageProviderOptions = new CommonStorageProviderOptions[] { this.AzureStorage, this.ServiceBus };
                var activeProviders = storageProviderOptions.Where(provider => provider?.HubName != null);
                if (activeProviders.Count() != 1)
                {
                    throw new InvalidOperationException("Must have one and only one storage provider configured.");
                }

                this.configuredProvider = activeProviders.First();
            }

            return this.configuredProvider;
        }

        internal void Validate()
        {
            this.GetConfiguredProvider().Validate();
        }
    }
}
