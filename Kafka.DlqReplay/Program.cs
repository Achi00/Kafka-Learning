using Confluent.Kafka;
using Kafka.Shared.Setup;
using System.Globalization;
using System.Text;

var groupId = args.Length > 0 ? args[0] : "dispatch";

// returns name for dql {groupId}.{topic}.dlq, same for "Retry"
var dlq = Topics.DeadLetter(groupId, Topics.DriverLocations);
var retry = Topics.Retry(groupId, Topics.DriverLocations);
const int maxReplays = 3;

using var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = KafkaDefaults.BootstrapServers,
    Acks = Acks.All,
    EnableIdempotence = true
}).Build();


using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = KafkaDefaults.BootstrapServers,
    GroupId = $"dlq-replayer.{groupId}",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    // tells if we have reached the end
    EnablePartitionEof = true
}).Build();

consumer.Subscribe(dlq);
int replayed = 0, gaveUp = 0;

while (true)
{
    var cr = consumer.Consume(TimeSpan.FromSeconds(10));
    // DLQ has 1 partition -> end reached
    if (cr is null || cr.IsPartitionEOF)
    {
        break;
    }

    // for header value to keep count
    var count = ReadInt(cr.Message.Headers, "dlq-replay-count");
    if (count >= maxReplays)
    {
        Console.WriteLine($"GAVE UP p{cr.Partition.Value} o{cr.Offset.Value} after {count} replays");
        consumer.Commit(cr);
        gaveUp++;
        continue;
    }


    // create headers to include additional count value
    var headers = new Headers();
    foreach (var h in cr.Message.Headers)
    {
        if (h.Key != "dlq-replay-count")
        {
            headers.Add(h.Key, h.GetValueBytes());
        }
    }
    headers.Add("dlq-replay-count", Encoding.UTF8.GetBytes((count + 1).ToString()));

    await producer.ProduceAsync(retry, new Message<string, string>
    {
        Key = cr.Message.Key,
        Value = cr.Message.Value,
        Headers = headers
    });
    Console.WriteLine($"REPLAY p{cr.Partition.Value} o{cr.Offset.Value} key={cr.Message.Key} count={count + 1}");

    // only after the retry write is confirmed
    consumer.Commit(cr);
    replayed++;
}

consumer.Close();
Console.WriteLine($"Replayed {replayed}, gave up on {gaveUp}");

// reading last bytes because it is most recent additions to headers history
// byte -> string -> try parse int
static int ReadInt(Headers? headers, string key) =>
    headers is not null && headers.TryGetLastBytes(key, out var b) && int.TryParse(Encoding.UTF8.GetString(b), out var n) ? n : 0;