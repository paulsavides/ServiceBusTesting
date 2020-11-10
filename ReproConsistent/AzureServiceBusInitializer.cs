using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproConsistent
{
  public static class AzureServiceBusInitializer
  {
    public static async Task InitializeQueuesAndStuff(AzureServiceBusConfiguration config)
    {
      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(config.KeyName, config.SharedAccessSignature, TimeSpan.FromDays(1));
      var managementClient = new ManagementClient(config.Endpoint, tokenProvider);

      if (!await managementClient.QueueExistsAsync(config.QueueName))
      {
        await managementClient.CreateQueueAsync(config.QueueName);
      }

      if (!await managementClient.TopicExistsAsync(config.TopicName))
      {
        await managementClient.CreateTopicAsync(config.TopicName);
      }

      if (!await managementClient.SubscriptionExistsAsync(config.TopicName, config.TopicName))
      {
        await managementClient.CreateSubscriptionAsync(new SubscriptionDescription(config.TopicName, config.TopicName)
        {
          ForwardTo = config.QueueName
        });
      }

      await managementClient.CloseAsync();
    }
  }
}
