using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproProject
{
  public class AzureServiceBusReceiver
  {
    private readonly IMessageReceiver _receiver;

    private AzureServiceBusReceiver(IMessageReceiver receiver)
    {
      _receiver = receiver;
    }

    public static AzureServiceBusReceiver Create(AzureServiceBusConfiguration config, Func<IMessageReceiver, Message, CancellationToken, Task> receiveFunc, Func<ExceptionReceivedEventArgs, Task> errorFunc)
    {
      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(config.KeyName, config.SharedAccessSignature, TimeSpan.FromDays(1));

      var queueConnection = new ServiceBusConnection(config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      {
        TokenProvider = tokenProvider
      };

      MessageReceiver receiver = null;
      if (config.AllowReceiverToOwnConnection)
      {
        receiver = new MessageReceiver(config.Endpoint, config.QueueName, tokenProvider, receiveMode: ReceiveMode.PeekLock, retryPolicy: RetryPolicy.Default)
        {
          PrefetchCount = config.MaxConcurrentCalls
        };
      }
      else
      {
        receiver = new MessageReceiver(queueConnection, config.QueueName, ReceiveMode.PeekLock, RetryPolicy.Default)
        {
          PrefetchCount = config.MaxConcurrentCalls
        };
      }

      receiver.RegisterMessageHandler((message, token) => receiveFunc(receiver, message, token), new MessageHandlerOptions(errorFunc)
      {
        AutoComplete = false,
        MaxConcurrentCalls = config.MaxConcurrentCalls
      });

      return new AzureServiceBusReceiver(receiver);
    }

    public Task CloseAsync()
    {
      return _receiver.CloseAsync();
    }

    public bool IsInnerReceiverClosedOrClosing()
    {
      return _receiver.IsClosedOrClosing;
    }

    public ReceivingAmqpLink GetCurrentlyOpenedLink()
    {
      var linkWrapper = ((MessageReceiver)_receiver).GetInternalProperty<MessageReceiver, FaultTolerantAmqpObject<ReceivingAmqpLink>>("ReceiveLinkManager");

      if (linkWrapper.TryGetOpenedObject(out var link))
      {
        return link;
      }

      return null;
    }

  }
}
