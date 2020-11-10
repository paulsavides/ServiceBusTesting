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

      receiver.RegisterMessageHandler((message, token) => ReceiveMessageAsync(receiver, message, token), new MessageHandlerOptions(HandleErrorAsync)
      {
        AutoComplete = false,
        MaxConcurrentCalls = config.MaxConcurrentCalls
      });

      return receiver;
    }

    public static async Task ReceiveMessageAsync(IMessageReceiver receiver, Message message, CancellationToken cancellationToken)
    {
      Exception receiveEx = null;
      try
      {
        cancellationToken.ThrowIfCancellationRequested();
        Console.WriteLine("Recieved MessageId=[{0}] MessageBody=[{1}]", message.MessageId, Encoding.UTF8.GetString(message.Body));
        await receiver.CompleteAsync(message.SystemProperties.LockToken);
      }
      catch (Exception ex)
      {
        receiveEx = ex;
        Console.WriteLine("Exception ocurred during ReceiveMessageAsync() Message=[{0}]", ex.Message);
      }

      if (receiveEx != null)
      {
        await receiver.AbandonAsync(message.SystemProperties.LockToken);
      }
    }

    public static Task HandleErrorAsync(ExceptionReceivedEventArgs args)
    {
      var ctx = args.ExceptionReceivedContext;
      var ex = args.Exception;

      Console.WriteLine("Action=[{0}] ClientId=[{1}] Endpoint=[{2}] EntityPath=[{3}] Exception=[{4}]", ctx.Action, ctx.ClientId, ctx.Endpoint, ctx.EntityPath, ex.Message);
      return Task.CompletedTask;
    }

    public static FaultTolerantAmqpObject<ReceivingAmqpLink> GetLinkFromReceiver(MessageReceiver receiver)
    {
      var property = receiver.GetType().GetProperty("ReceiveLinkManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
      object linkObj = property.GetValue(receiver);
      return linkObj as FaultTolerantAmqpObject<ReceivingAmqpLink>;
    }
  }
}
