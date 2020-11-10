using System;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproConsistent
{
  public class AzureServiceBusManagementClientWrapper
  {
    private readonly AzureServiceBusConfiguration _config;
    private readonly ManagementClient _client;

    private Timer _timer;

    public AzureServiceBusManagementClientWrapper(AzureServiceBusConfiguration config)
    {
      _config = config;

      var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(config.KeyName, config.SharedAccessSignature, TimeSpan.FromDays(1));
      _client = new ManagementClient(config.Endpoint, tokenProvider);
    }

    public async Task IntializeEntities()
    {
      if (!await _client.QueueExistsAsync(_config.QueueName))
      {
        await _client.CreateQueueAsync(_config.QueueName);
      }

      if (!await _client.TopicExistsAsync(_config.TopicName))
      {
        await _client.CreateTopicAsync(_config.TopicName);
      }

      if (!await _client.SubscriptionExistsAsync(_config.TopicName, _config.TopicName))
      {
        await _client.CreateSubscriptionAsync(new SubscriptionDescription(_config.TopicName, _config.TopicName)
        {
          ForwardTo = _config.QueueName
        });
      }
    }

    public void StartSimulatingTransientErrors()
    {
      _timer = new Timer(_config.TransientErrorInterval);
      _timer.Elapsed += UpdateQueueAsync;

      _timer.Start();
    }

    private Random _rand = new Random();
    private async void UpdateQueueAsync(object sender, ElapsedEventArgs args)
    {
      try
      {
        var queue = await _client.GetQueueAsync(_config.QueueName);
        queue.DefaultMessageTimeToLive = TimeSpan.FromMinutes(_rand.Next(0, 1000));

        await _client.UpdateQueueAsync(queue);
      }
      catch(Exception ex)
      {
        Console.WriteLine("Unable to update queue message=" + ex.Message);
      }
    }

    public Task Shutdown()
    {
      _timer?.Stop();
      return _client.CloseAsync();
    }
  }
}
