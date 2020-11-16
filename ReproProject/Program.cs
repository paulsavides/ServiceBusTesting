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
        PublishInterval = 5000,
        AllowReceiverToOwnConnection = true
      };

      var wrapper = await AzureServiceBusWrapper.InitializeAsync(config);

      bool shutdown = false;
      while (!shutdown)
      {
        Console.WriteLine("Press d to print diagnostics or press any other key to shut down and exit...");
        var key = Console.ReadKey();
        Console.WriteLine();
        if (key.KeyChar == 'd')
        {
          await wrapper.PrintRecycledReceiversDiagnosticsAsync();
        }
        else
        {
          shutdown = true;
        }
      }

      Console.WriteLine("Shutting down...");
      await wrapper.ShutdownAsync();
    }
  }
}
