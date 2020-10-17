using MassTransit;
using MassTransit.Context;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using System.Timers;

namespace ReproProjectMassTransit
{
  public static class Program
  {
    public static async Task Main()
    {
      var config = new MTASBSettings
      {
        MaxConcurrentCalls = 100,
        Endpoint = "sb://endpoint.servicebus.windows.net",
        KeyName = "RootManageSharedAccessKey",
        SharedAccessSignature = "setme",
        PublishInterval = 5000
      };

      var loggerFactory = new ServiceCollection()
        .AddLogging(loggingOptions => loggingOptions.AddConsole().SetMinimumLevel(LogLevel.Trace).AddFile(@"C:\logs\repro.mt.{Date}.log", minimumLevel: LogLevel.Trace))
        .BuildServiceProvider()
        .GetRequiredService<ILoggerFactory>();

      LogContext.ConfigureCurrentLogContext(loggerFactory);

      var busControl = Bus.Factory.CreateUsingAzureServiceBus(busConfigurator =>
      {
        busConfigurator.Host(config.Endpoint, hostConfig =>
        {
          hostConfig.SharedAccessSignature(sasConfig =>
          {
            sasConfig.KeyName = config.KeyName;
            sasConfig.SharedAccessKey = config.SharedAccessSignature;
            sasConfig.TokenTimeToLive = TimeSpan.FromDays(1);
          });
        });

        busConfigurator.ReceiveEndpoint("repro.project.mass.transit.queue", ep =>
        {
          ep.MaxConcurrentCalls = config.MaxConcurrentCalls;
          ep.LockDuration = TimeSpan.FromMinutes(1);
          ep.Consumer<Consumer>(() => new Consumer(loggerFactory.CreateLogger<Consumer>()));
        });
      });

      await busControl.StartAsync();

      var publisher = new Publisher(busControl, loggerFactory.CreateLogger<Publisher>());

      var timer = new System.Timers.Timer(config.PublishInterval);
      timer.Elapsed += publisher.Publish;

      timer.Start();

      Console.WriteLine("Press any key to exit...");
      Console.ReadKey();

      timer.Stop();
      await busControl.StopAsync();
    }
  }

  public class MTASBSettings
  {
    public int MaxConcurrentCalls { get; set; }
    public string Endpoint { get; set; }
    public string KeyName { get; set; }
    public string SharedAccessSignature { get; set; }
    public int PublishInterval { get; set; }
  }

  public class TestMessage
  {
    public string Message { get; set; }
  }

  public class Consumer : IConsumer<TestMessage>
  {
    private readonly ILogger<Consumer> _logger;
    public Consumer(ILogger<Consumer> logger) { _logger = logger; }
    public Task Consume(ConsumeContext<TestMessage> context)
    {
      _logger.LogTrace("Received Message={0}", context.Message.Message);
      return Task.CompletedTask;
    }
  }

  public class Publisher
  {
    private readonly IPublishEndpoint _endpoint;
    private readonly ILogger<Publisher> _logger;
    public Publisher(IPublishEndpoint endpoint, ILogger<Publisher> logger)
    {
      _endpoint = endpoint;
      _logger = logger;
    }

    public async void Publish(object sender, ElapsedEventArgs elapsed)
    {
      var guid = Guid.NewGuid().ToString();
      _logger.LogTrace("Publishing Message={0}", guid);
      await _endpoint.Publish<TestMessage>(new TestMessage
      {
        Message = guid
      });
    }
  }
}
