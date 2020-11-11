using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproConsistent
{
  public class AzureServiceBusMessageReceiver
  {
    private readonly AzureServiceBusConfiguration _config;
    private readonly MessageReceiver _receiver;

    private bool _foundError = false;

    public string ClientId => _receiver.ClientId;

    public AzureServiceBusMessageReceiver(AzureServiceBusConfiguration config)
    {
      _config = config;
      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(config.KeyName, config.SharedAccessSignature, TimeSpan.FromDays(1));

      var queueConnection = new ServiceBusConnection(config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      {
        TokenProvider = tokenProvider
      };

      _receiver = new MessageReceiver(queueConnection, config.QueueName, ReceiveMode.PeekLock, RetryPolicy.Default)
      {
        PrefetchCount = config.MaxConcurrentCalls
      };

      _receiver.RegisterMessageHandler(ReceiveMessageAsync, new MessageHandlerOptions(HandleErrorAsync)
      {
        AutoComplete = false,
        MaxConcurrentCalls = config.MaxConcurrentCalls
      });
    }

    public async Task ReceiveMessageAsync(Message message, CancellationToken cancellationToken)
    {
      Exception receiveEx = null;
      try
      {
        cancellationToken.ThrowIfCancellationRequested();
        Console.WriteLine("Received MessageId=[{0}] MessageBody=[{1}]", message.MessageId, Encoding.UTF8.GetString(message.Body));
        await _receiver.CompleteAsync(message.SystemProperties.LockToken);
      }
      catch (Exception ex)
      {
        receiveEx = ex;
        Console.WriteLine("Exception ocurred during ReceiveMessageAsync() Message=[{0}]", ex.Message);
      }

      if (receiveEx != null)
      {
        await _receiver.AbandonAsync(message.SystemProperties.LockToken);
      }
    }

    public Task HandleErrorAsync(ExceptionReceivedEventArgs args)
    {
      var ctx = args.ExceptionReceivedContext;
      var ex = args.Exception;

      Console.WriteLine("Action=[{0}] ClientId=[{1}] Endpoint=[{2}] EntityPath=[{3}] Exception=[{4}]", ctx.Action, ctx.ClientId, ctx.Endpoint, ctx.EntityPath, ex.Message);
      _foundError = true;
      return Task.CompletedTask;
    }

    public FaultTolerantAmqpObject<ReceivingAmqpLink> GetLink()
    {
      var property = _receiver.GetType().GetProperty("ReceiveLinkManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
      object linkObj = property.GetValue(_receiver);
      return linkObj as FaultTolerantAmqpObject<ReceivingAmqpLink>;
    }

    public async Task WaitForError(CancellationToken token)
    {
      while (!token.IsCancellationRequested && !_foundError)
      {
        await Task.Delay(10);
      }
    }

    public Task Shutdown()
    {
      return _receiver.CloseAsync();
    }
  }
}
