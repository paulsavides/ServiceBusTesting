using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.ServiceBus.Primitives;
using System;
using System.Threading;
using System.Threading.Tasks;

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

      //var connection = new ServiceBusConnection(_config.Endpoint, TransportType.Amqp, RetryPolicy.Default)
      //{
      //  TokenProvider = tokenProvider
      //};
    }

    public async Task ShutdownAsync(CancellationToken cancellationToken = default)
    {
      cancellationToken.ThrowIfCancellationRequested();

      await _managementClient.CloseAsync();
    }
  }
}
