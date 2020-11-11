using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproProject
{
  public class AzureServiceBusConfiguration
  {
    public string Endpoint { get; set; }
    public string QueueName { get; set; }
    public string TopicName { get; set; }
    public int MaxConcurrentCalls { get; set; }
    public string SharedAccessSignature { get; set; }
    public string KeyName { get; set; }
    public int PublishInterval { get; set; }
  }

  public class AzureServiceBusWrapper
  {
    private AzureServiceBusWrapper() { }
    public static async Task<AzureServiceBusWrapper> InitializeAsync(AzureServiceBusConfiguration configuration, CancellationToken cancellationToken = default)
    {
      var wrapper = new AzureServiceBusWrapper();
      await wrapper.InitializeInternalAsync(configuration, cancellationToken);
      return wrapper;
    }

    private AzureServiceBusConfiguration _config;
    private ManagementClient _managementClient;
    private AzureServiceBusReceiver _messageReceiver;
    private ISenderClient _messageSender;
    private System.Timers.Timer _messageSendTimer;

    private readonly SemaphoreSlim _recycleLock = new SemaphoreSlim(1);
    private readonly List<AzureServiceBusReceiver> _closedReceivers = new List<AzureServiceBusReceiver>();

    private async Task InitializeInternalAsync(AzureServiceBusConfiguration configuration, CancellationToken cancellationToken)
    {
      cancellationToken.ThrowIfCancellationRequested();
      _config = configuration;

      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(_config.KeyName, _config.SharedAccessSignature, TimeSpan.FromDays(1));
      _managementClient = new ManagementClient(_config.Endpoint, tokenProvider);

      if (!await _managementClient.QueueExistsAsync(_config.QueueName, cancellationToken))
      {
        await _managementClient.CreateQueueAsync(_config.QueueName, cancellationToken);
      }

      if (!await _managementClient.TopicExistsAsync(_config.TopicName, cancellationToken))
      {
        await _managementClient.CreateTopicAsync(_config.TopicName, cancellationToken);
      }

      if (!await _managementClient.SubscriptionExistsAsync(_config.TopicName, _config.TopicName, cancellationToken))
      {
        await _managementClient.CreateSubscriptionAsync(new SubscriptionDescription(_config.TopicName, _config.TopicName)
        {
          ForwardTo = _config.QueueName
        }, cancellationToken);
      }

      var queueConnection = new ServiceBusConnection(_config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      {
        TokenProvider = tokenProvider
      };

      _messageReceiver = AzureServiceBusReceiver.Create(_config, ReceiveMessageAsync, HandleErrorAsync);

      var topicConnection = new ServiceBusConnection(_config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      {
        TokenProvider = tokenProvider
      };

      _messageSender = new TopicClient(topicConnection, _config.TopicName, RetryPolicy.Default);
      _messageSendTimer = new System.Timers.Timer(_config.PublishInterval);
      _messageSendTimer.Elapsed += SendMessageAsync;

      _messageSendTimer.Start();
    }

    public async Task ShutdownAsync(CancellationToken cancellationToken = default)
    {
      cancellationToken.ThrowIfCancellationRequested();
      _messageSendTimer.Stop();
      await _managementClient.CloseAsync();
      await _messageReceiver.CloseAsync();
      await _messageSender.CloseAsync();
    }

    public async Task ReceiveMessageAsync(IMessageReceiver receiver, Message message, CancellationToken cancellationToken)
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

    public async void SendMessageAsync(object sender, ElapsedEventArgs args)
    {
      var messageId = Guid.NewGuid().ToString();
      Console.WriteLine("Sending MessageId=[{0}]", messageId);
      await _messageSender.SendAsync(new Message
      {
        MessageId = messageId,
        Body = Encoding.UTF8.GetBytes("hello!")
      });
    }

    public Task HandleErrorAsync(ExceptionReceivedEventArgs args)
    {
      var ctx = args.ExceptionReceivedContext;
      var ex = args.Exception;

      Console.WriteLine("Action=[{0}] ClientId=[{1}] Endpoint=[{2}] EntityPath=[{3}] Exception=[{4}]", ctx.Action, ctx.ClientId, ctx.Endpoint, ctx.EntityPath, ex.Message);
      Task.Run(() => RecyleReceiverAsync());
      return Task.CompletedTask;
    }

    private async Task RecyleReceiverAsync()
    {
      try
      {
        await _recycleLock.WaitAsync();
        Console.WriteLine("Recycling the Receiver");

        await _messageReceiver.CloseAsync();
        _closedReceivers.Add(_messageReceiver);
        _messageReceiver = AzureServiceBusReceiver.Create(_config, ReceiveMessageAsync, HandleErrorAsync);

      }
      finally
      {
        _recycleLock.Release();
      }
    }
  }
}
