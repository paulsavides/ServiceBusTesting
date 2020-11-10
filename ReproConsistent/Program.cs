using System;
using System.Threading.Tasks;

namespace ReproConsistent
{
  public static class Program
  {
    public static async Task Main()
    {
      var config = new AzureServiceBusConfiguration
      {
        QueueName = "repro.consistent.queue",
        TopicName = "repro.consistent.topic",
        MaxConcurrentCalls = 10,
        Endpoint = "sb://mybus.servicebus.windows.net",
        KeyName = "RootManageSharedAccessKey",
        SharedAccessSignature = "replaceme",
        PublishInterval = 5000
      };

      await AzureServiceBusInitializer.InitializeQueuesAndStuff(config);

      Console.WriteLine("Press any key to shut down and exit...");
      Console.ReadKey();
    }
  }
}
