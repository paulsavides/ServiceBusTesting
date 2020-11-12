using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.ServiceBus.Primitives;

namespace ReproConsistent
{
  public class AzureServiceBusManagementClientWrapper
  {
    private readonly AzureServiceBusConfiguration _config;
    private readonly ManagementClient _client;
    private readonly CancellationTokenSource _errorCts = new CancellationTokenSource();
    private Task _errorTask;

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
      _errorTask = UpdateQueueAsync(_errorCts.Token);
    }

    private readonly Random _rand = new Random();
    private async Task UpdateQueueAsync(CancellationToken token)
    {
      while (!token.IsCancellationRequested)
      {
        try
        {
          var queue = await _client.GetQueueAsync(_config.QueueName);

          queue.DefaultMessageTimeToLive = TimeSpan.FromMinutes(_rand.Next(5, 1000));
          queue.AutoDeleteOnIdle = TimeSpan.FromMinutes(_rand.Next(5, 1000));

          await _client.UpdateQueueAsync(queue);

          await Task.Delay(_config.TransientErrorInterval);
        }
        catch (Exception ex)
        {
          Console.WriteLine("Unable to update queue, error=" + ex.Message);
        }
      }
    }

    public async Task Shutdown()
    {

      _errorCts.Cancel();

      if (_errorTask != null)
      {
        await _errorTask;
      }

      await _client.CloseAsync();
    }
  }
}
