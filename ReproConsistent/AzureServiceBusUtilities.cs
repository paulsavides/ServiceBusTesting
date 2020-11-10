using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproConsistent
{
  public static class AzureServiceBusUtilities
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

    public static MessageReceiver CreateMessageReceiver(AzureServiceBusConfiguration config)
    {
      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(config.KeyName, config.SharedAccessSignature, TimeSpan.FromDays(1));

      var queueConnection = new ServiceBusConnection(config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      {
        TokenProvider = tokenProvider
      };

      var receiver = new MessageReceiver(queueConnection, config.QueueName, ReceiveMode.PeekLock, RetryPolicy.Default)
      {
        PrefetchCount = config.MaxConcurrentCalls
      };

      receiver.RegisterMessageHandler((message, token) => Task.CompletedTask, new MessageHandlerOptions(args => Task.CompletedTask)
      {
        AutoComplete = false,
        MaxConcurrentCalls = config.MaxConcurrentCalls
      });

      return receiver;
    }

    public static FaultTolerantAmqpObject<ReceivingAmqpLink> GetLinkFromReceiver(MessageReceiver receiver)
    {
      var property = receiver.GetType().GetProperty("ReceiveLinkManager", System.Reflection.BindingFlags.NonPublic);
      object linkObj = property.GetValue(receiver);
      return linkObj as FaultTolerantAmqpObject<ReceivingAmqpLink>;
    }
  }
}
