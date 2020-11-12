﻿using System;
using System.Linq;
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
    private readonly MessageReceiver _receiver;
    private readonly ServiceBusConnection _connection;
    private bool _foundError = false;

    public string ClientId => _receiver.ClientId;

    public AzureServiceBusMessageReceiver(AzureServiceBusConfiguration config)
    {
      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(config.KeyName, config.SharedAccessSignature, TimeSpan.FromDays(1));

      _connection = new ServiceBusConnection(config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      {
        TokenProvider = tokenProvider
      };

      _receiver = new MessageReceiver(_connection, config.QueueName, ReceiveMode.PeekLock, RetryPolicy.Default)
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

      bool shouldShutdown = !((ex is ServiceBusException sbException && sbException.IsTransient) || ex is MessageLockLostException);

      Console.WriteLine("ShouldShutdown=[{0}] Action=[{1}] ClientId=[{2}] Endpoint=[{3}] EntityPath=[{4}] Exception=[{5}]", shouldShutdown,
        ctx.Action, ctx.ClientId, ctx.Endpoint, ctx.EntityPath, ex.Message);

      _foundError = shouldShutdown;
      return Task.CompletedTask;
    }

    public FaultTolerantAmqpObject<ReceivingAmqpLink> GetLinkManager()
      => GetInternalProperty<MessageReceiver, FaultTolerantAmqpObject<ReceivingAmqpLink>>(_receiver, "ReceiveLinkManager");

    public FaultTolerantAmqpObject<AmqpConnection> GetAmqpConnectionManager()
      => GetInternalProperty<ServiceBusConnection, FaultTolerantAmqpObject<AmqpConnection>>(_connection, "ConnectionManager");

    private static TProp GetInternalProperty<TFrom, TProp>(TFrom from, string propertyName)
      where TProp : class
    {
      var property = from.GetType().GetProperty(propertyName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
      return property.GetValue(from) as TProp;
    }

    public async Task WaitForError(params CancellationToken[] tokens)
    {
      while (!tokens.Any(t => t.IsCancellationRequested) && !_foundError)
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
