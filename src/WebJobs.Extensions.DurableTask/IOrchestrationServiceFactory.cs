using System;
using System.Collections.Generic;
using System.Text;
using DurableTask.Core;
using Microsoft.Azure.WebJobs.Extensions.DurableTask.Options;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask
{
    public interface IOrchestrationServiceFactory
    {
        IOrchestrationService GetOrchestrationService();

        IOrchestrationServiceClient GetOrchestrationClient(OrchestrationClientAttribute attribute);
    }
}
