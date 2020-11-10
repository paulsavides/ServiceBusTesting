using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproConsistent
{
  public class AzureServiceBusMessagePublisher
  {
    private readonly AzureServiceBusConfiguration _config;
    private readonly IMessageSender _sender;
    private readonly CancellationTokenSource _sendCts = new CancellationTokenSource();
    private readonly Task _sendTask;

    public AzureServiceBusMessagePublisher(AzureServiceBusConfiguration config)
    {
      _config = config;
      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(config.KeyName, config.SharedAccessSignature, TimeSpan.FromDays(1));

      var topicConnection = new ServiceBusConnection(config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      {
        TokenProvider = tokenProvider
      };

      _sender = new MessageSender(topicConnection, config.TopicName);
      _sendTask = SendMessageAsync(_sendCts.Token);
    }

    private async Task SendMessageAsync(CancellationToken token)
    {
      while (!token.IsCancellationRequested)
      {
        try
        {
          var messageId = Guid.NewGuid().ToString();
          Console.WriteLine("Sending MessageId=[{0}]", messageId);
          await _sender.SendAsync(new Message
          {
            MessageId = messageId,
            Body = Encoding.UTF8.GetBytes("hello!")
          });

          await Task.Delay(_config.PublishInterval);
        }
        catch (Exception ex)
        {
          Console.WriteLine("Encountered an error publishing message, error=" + ex.Message);
        }
      }
    }

    public async Task Shutdown()
    {
      _sendCts.Cancel();
      await _sendTask;
      await _sender.CloseAsync();
    }
  }
}
