namespace ReproConsistent
{
  public class AzureServiceBusConfiguration
  {
    public string Endpoint { get; set; }
    public string QueueName { get; set; }
    public string TopicName { get; set; }
    public int MaxConcurrentCalls { get; set; }
    public string SharedAccessSignature { get; set; }
    public string KeyName { get; set; }
    public int TransientErrorInterval { get; set; }
    public int PublishInterval { get; set; }
  }
}
