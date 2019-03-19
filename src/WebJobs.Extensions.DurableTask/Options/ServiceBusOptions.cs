using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask.Options
{
    public class ServiceBusOptions : CommonStorageProviderOptions
    {
        //
        // Summary:
        //     Maximum number of times the task orchestration dispatcher will try to process
        //     an orchestration message before giving up
        public int? MaxTaskOrchestrationDeliveryCount { get; set; }

        //
        // Summary:
        //     Maximum number of times the task activity dispatcher will try to process an orchestration
        //     message before giving up
        public int? MaxTaskActivityDeliveryCount { get; set; }

        //
        // Summary:
        //     Maximum number of times the tracking dispatcher will try to process an orchestration
        //     message before giving up
        public int? MaxTrackingDeliveryCount { get; set; }

        //
        // Summary:
        //     Maximum queue size, in megabytes, for the service bus queues
        public long? MaxQueueSizeInMegabytes { get; set; }

        public string InstanceTableConnectionStringName { get; set; }

        public string BlobConnectionStringName { get; set; }
    }
}
