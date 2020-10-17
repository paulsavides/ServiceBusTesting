using System;
using System.Threading.Tasks;

namespace ReproProject
{
  public static class Program
  {
    public static async Task Main()
    {
      var config = new AzureServiceBusConfiguration
      {
        QueueName = "repro.project.queue",
        TopicName = "repro.project.topic",
        MaxConcurrentCalls = 10,
        Endpoint = "sb://endpoint.servicebus.windows.net",
        KeyName = "RootManageSharedAccessKey",
        SharedAccessSignature = "setme",
        PublishInterval = 5000
      };

      var wrapper = await AzureServiceBusWrapper.InitializeAsync(config);

      Console.WriteLine("Press any key to shut down and exit...");
      Console.ReadKey();

      await wrapper.ShutdownAsync();
    }
  }
}
