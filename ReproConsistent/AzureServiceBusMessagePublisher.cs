using System;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproConsistent
{
  public class AzureServiceBusMessagePublisher
  {
    private readonly IMessageSender _sender;
    private readonly Timer _timer;

    public AzureServiceBusMessagePublisher(AzureServiceBusConfiguration config)
    {
      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(config.KeyName, config.SharedAccessSignature, TimeSpan.FromDays(1));

      var topicConnection = new ServiceBusConnection(config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      {
        TokenProvider = tokenProvider
      };

      _sender = new MessageSender(topicConnection, config.TopicName);
      _timer = new Timer(config.PublishInterval);
      _timer.Elapsed += SendMessageAsync;

      _timer.Start();
    }

    private async void SendMessageAsync(object sender, ElapsedEventArgs args)
    {
      var messageId = Guid.NewGuid().ToString();
      Console.WriteLine("Sending MessageId=[{0}]", messageId);
      await _sender.SendAsync(new Message
      {
        MessageId = messageId,
        Body = Encoding.UTF8.GetBytes("hello!")
      });
    }

    public Task Shutdown()
    {
      _timer.Stop();
      return _sender.CloseAsync();
    }
  }
}
